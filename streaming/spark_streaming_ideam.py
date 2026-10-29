#!/usr/bin/env python3
"""
Tarea 3 - Big Data (UNAD, 202016911)
Consumidor Spark Structured Streaming: lee del topic de Kafka las lecturas de temperatura
del IDEAM (Huila) que envia kafka_producer_ideam.py y las procesa en tiempo real.

Procesamiento:
  1) Monitor de cada micro-lote: cantidad de mensajes, estaciones, distribucion por particion
     de Kafka, temperatura media/min/max, latencia media y alertas de temperatura alta.
  2) Estadisticas por ventana de tiempo del evento (por defecto 6 horas) y por estacion:
     lecturas, media, minimo y maximo. Se muestran las 5 estaciones mas calidas de la ventana
     mas reciente y todo se guarda en CSV para graficarlo despues.

Uso (primero este consumidor, luego el productor en otra terminal):
    spark-submit --driver-memory 1g \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
      spark_streaming_ideam.py [latest|earliest]

Se detiene con Ctrl+C. Los resultados quedan en ./salida_streaming/
"""
import os
import shutil
import sys
import threading
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# ------------------------------- PARAMETROS ---------------------------------
BROKER = "localhost:9092"
TOPIC = "ideam_temperatura"
UMBRAL_ALTA = float(os.environ.get("UMBRAL", "35.0"))   # C: lectura que genera alerta
VENTANA = "6 hours"                                      # ventana de tiempo del evento
DISPARO = os.environ.get("DISPARO", "5 seconds")         # cada cuanto se procesa un micro-lote
SALIDA = "salida_streaming"
CKPT = "/tmp/ckpt_ideam"
INICIO = sys.argv[1] if len(sys.argv) > 1 else "latest"  # latest | earliest
FUENTE_JSON = os.environ.get("FUENTE_JSON")              # solo para pruebas sin Kafka
# -----------------------------------------------------------------------------

for d in (SALIDA, CKPT):                                 # cada ejecucion parte de cero
    shutil.rmtree(d, ignore_errors=True)

spark = (
    SparkSession.builder.appName("IDEAM_Streaming")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("sensor_id", StringType()),
    StructField("estacion", StringType()),
    StructField("municipio", StringType()),
    StructField("temperature", DoubleType()),
    StructField("event_time", StringType()),
    StructField("enviado", DoubleType()),
])

# ---------------------------------------------------------------- utilidades de impresion
CANDADO = threading.Lock()


def emitir(texto):
    """Imprime un bloque completo, para que las dos consultas no mezclen sus lineas."""
    with CANDADO:
        print(texto, flush=True)


def texto_tabla(df, n=5):
    filas = df.limit(n).collect()
    cols = df.columns
    datos = [[str(r[c]) for c in cols] for r in filas]
    anchos = [max([len(c)] + [len(f[i]) for f in datos]) for i, c in enumerate(cols)]
    def lin(vals):
        return "   " + " | ".join(v.ljust(a) for v, a in zip(vals, anchos))
    return "\n".join([lin(cols), "   " + "-+-".join("-" * a for a in anchos)] + [lin(f) for f in datos])


# ---------------------------------------------------------------- fuente
if FUENTE_JSON:   # modo de prueba: lee archivos JSON en vez de Kafka
    crudo = (spark.readStream.schema(esquema).option("maxFilesPerTrigger", 1).json(FUENTE_JSON)
             .withColumn("particion", F.lit(-1)))
else:
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", INICIO)
        .option("failOnDataLoss", "false")
        .load()
    )
    crudo = (
        kafka.select(F.from_json(F.col("value").cast("string"), esquema).alias("d"),
                     F.col("partition").alias("particion"))
        .select("d.*", "particion")
    )

eventos = crudo.withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))

# ---------------------------------------------------------------- 1) monitor por micro-lote
acumulado = {"mensajes": 0, "alertas": 0}


def monitor(df, batch_id):
    if df.isEmpty():
        return
    df = df.cache()
    n = df.count()
    r = df.agg(
        F.countDistinct("sensor_id").alias("estaciones"),
        F.round(F.avg("temperature"), 2).alias("media"),
        F.round(F.min("temperature"), 1).alias("minimo"),
        F.round(F.max("temperature"), 1).alias("maximo"),
        F.date_format(F.min("event_time"), "yyyy-MM-dd HH:mm").alias("desde"),
        F.date_format(F.max("event_time"), "yyyy-MM-dd HH:mm").alias("hasta"),
        F.round(F.avg(F.lit(time.time()) - F.col("enviado")), 2).alias("latencia"),
    ).first()
    partes = {int(x["particion"]): x["n"] for x in df.groupBy("particion").agg(F.count("*").alias("n")).collect()}
    acumulado["mensajes"] += n
    texto = [
        f"\n[Lote {batch_id}] {n} mensajes | {r['estaciones']} estaciones | particiones {dict(sorted(partes.items()))}",
        f"   Eventos de {r['desde']} a {r['hasta']} | temperatura media {r['media']} C "
        f"(min {r['minimo']}, max {r['maximo']}) | latencia media {r['latencia']} s | acumulado: {acumulado['mensajes']}",
    ]
    alertas = (df.filter(F.col("temperature") >= UMBRAL_ALTA)
                 .select("estacion", F.date_format("event_time", "yyyy-MM-dd HH:mm").alias("event_time"), "temperature"))
    n_al = alertas.count()
    if n_al:
        acumulado["alertas"] += n_al
        texto.append(f"   ALERTA: {n_al} lecturas >= {UMBRAL_ALTA} C (alertas acumuladas: {acumulado['alertas']})")
        texto.append(texto_tabla(alertas.orderBy(F.desc("temperature")), 5))
        alertas.withColumn("lote", F.lit(batch_id)).write.mode("append").option("header", True).csv(f"{SALIDA}/alertas")
    (df.groupBy("particion").agg(F.count("*").alias("mensajes")).withColumn("lote", F.lit(batch_id))
       .write.mode("append").option("header", True).csv(f"{SALIDA}/particiones"))
    df.unpersist()
    emitir("\n".join(texto))


q_monitor = (
    eventos.writeStream.foreachBatch(monitor)
    .option("checkpointLocation", f"{CKPT}/monitor")
    .trigger(processingTime=DISPARO)
    .start()
)

# ---------------------------------------------------------------- 2) ventanas por estacion
ventanas = (
    eventos.withWatermark("event_time", "12 hours")
    .groupBy(F.window("event_time", VENTANA), "estacion")
    .agg(
        F.count("*").alias("lecturas"),
        F.round(F.avg("temperature"), 2).alias("media"),
        F.round(F.min("temperature"), 1).alias("minimo"),
        F.round(F.max("temperature"), 1).alias("maximo"),
    )
)


def guardar_ventanas(df, batch_id):
    if df.isEmpty():
        return
    planas = df.select(
        F.date_format("window.start", "yyyy-MM-dd HH:mm").alias("inicio"),
        F.date_format("window.end", "yyyy-MM-dd HH:mm").alias("fin"),
        "estacion", "lecturas", "media", "minimo", "maximo",
    ).cache()
    reciente = planas.agg(F.max("inicio")).first()[0]
    top = planas.filter(F.col("inicio") == reciente).orderBy(F.desc("media"))
    emitir(f"   Ventanas de {VENTANA} (actualizacion {batch_id}): 5 estaciones mas calidas de la ventana "
           f"que inicia {reciente}\n" + texto_tabla(top.drop("fin"), 5))
    planas.withColumn("lote", F.lit(batch_id)).write.mode("append").option("header", True).csv(f"{SALIDA}/ventanas")
    planas.unpersist()


q_ventanas = (
    ventanas.writeStream.outputMode("update")
    .foreachBatch(guardar_ventanas)
    .option("checkpointLocation", f"{CKPT}/ventanas")
    .trigger(processingTime=DISPARO)
    .start()
)

print(f"Consumidor iniciado. Fuente: {'JSON ' + FUENTE_JSON if FUENTE_JSON else TOPIC + ' en ' + BROKER} "
      f"| inicio: {INICIO} | ventana: {VENTANA} | alerta >= {UMBRAL_ALTA} C | lote cada {DISPARO}")
print("Esperando datos... (ejecuta el productor en otra terminal; Ctrl+C para detener)")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nDeteniendo consultas...")
    for q in (q_monitor, q_ventanas):
        q.stop()
    print(f"Total procesado: {acumulado['mensajes']} mensajes, {acumulado['alertas']} alertas. "
          f"Resultados en ./{SALIDA}/")
finally:
    spark.stop()
