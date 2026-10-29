#!/usr/bin/env python3
"""
Tarea 3 - Big Data (UNAD, 202016911)
Procesamiento en BATCH con Apache Spark (DataFrames + RDD)

Dataset : IDEAM - Temperatura del aire a 2 m, departamento del Huila, 2024-2025
Fuente  : datos.gov.co (ID sbwg-7ju4), descargado con descargar_ideam_huila.py

NOTA: el script incluye una normalizacion de la hora (seccion 2.7) INFERIDA del propio
dato: no esta confirmada por el IDEAM. Ver parametros DESPLAZ, MARGEN y REF_EST.

Ejecucion (desde la carpeta que contiene data/):
    spark-submit --driver-memory 1g batch_ideam_spark.py
    spark-submit --driver-memory 1g batch_ideam_spark.py ruta/entrada.csv ruta/salida

Salidas en ./salida :
    graficos/*.png          graficos del analisis
    tablas/*.csv            tablas resumen (para el informe)
    datos_limpios_parquet/  datos limpios en formato columnar
    ideam_horario_limpio.csv  datos limpios ordenados por fecha (para el productor de Kafka)
"""
import os
import sys

import matplotlib
matplotlib.use("Agg")  # sin entorno grafico: se guardan PNG, no se abren ventanas
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ------------------------------- PARAMETROS ---------------------------------
ENTRADA = sys.argv[1] if len(sys.argv) > 1 else "data/ideam_temperatura_huila_2024_2025.csv"
SALIDA = sys.argv[2] if len(sys.argv) > 2 else "salida"

# Rango fisicamente plausible para temperatura del aire a 2 m en el Huila.
# Se justifica con el perfilado: hay lecturas de 0,0 a 1,6 C en estaciones
# cuya media es de 20 a 28 C, y ninguna estacion tiene lecturas validas < 8 C.
TEMP_MIN, TEMP_MAX = 5.0, 45.0

# Codigos que corresponden a la misma estacion fisica (mismas coordenadas y, en las
# horas en que se solapan, valores identicos: ver perfil6.py). Clave = codigo que se
# reemplaza, valor = codigo que se conserva.
UNIFICAR_CODIGOS = {"0021115160": "0021115180"}   # HACIENDA MANILA

# Normalizacion de la base horaria (INFERIDA, no confirmada por el IDEAM).
# El perfilado mostro que hasta el 28/10/2024 las estaciones de mayor cobertura
# reportan en hora local (minimo ~6 h, maximo ~16 h) y desde el 29/10/2024 la
# mayoria parece reportar con el ciclo desplazado +5 h (consistente con UTC).
DESPLAZ = 5                       # horas entre la base "U" y la hora local de Colombia
MARGEN = 0.15                     # diferencia minima de correlacion para clasificar un dia
REF_EST = ["ZULUAGA", "SANTA MARIA", "ARGENTINA LA", "LIBANO EL", "ROSALES LOS", "POTOSI HACIENDA"]
REF_INI, REF_FIN = "2024-01-15", "2024-10-15"   # periodo en que esas estaciones salen todas en hora local
# -----------------------------------------------------------------------------


def titulo(texto):
    print("\n" + "=" * 78)
    print(texto)
    print("=" * 78)


spark = (
    SparkSession.builder.appName("IDEAM_Huila_Batch")
    .config("spark.sql.session.timeZone", "UTC")  # evita corrimientos de hora
    .config("spark.sql.shuffle.partitions", "8")  # 200 por defecto es excesivo para ~200 mil filas
    .config("spark.ui.showConsoleProgress", "false")  # salida limpia (sin barras [Stage ...])
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

for sub in ("graficos", "tablas"):
    os.makedirs(os.path.join(SALIDA, sub), exist_ok=True)

# =============================================================================
# 1. CARGA
# =============================================================================
titulo("1. CARGA DEL CONJUNTO DE DATOS")
raw = spark.read.csv(ENTRADA, header=True)  # todo como texto; se convierte por nombre
n_raw = raw.count()
print(f"Archivo: {ENTRADA}")
print(f"Filas leidas: {n_raw:,}   Columnas: {len(raw.columns)}")
raw.printSchema()

# =============================================================================
# 2. LIMPIEZA Y TRANSFORMACION
# =============================================================================
titulo("2. LIMPIEZA Y TRANSFORMACION")

# 2.1 Duplicados exactos
dedup = raw.dropDuplicates()
n_dedup = dedup.count()
print(f"[2.1] Duplicados exactos eliminados: {n_raw - n_dedup:,}")

# 2.2 Tipos de dato
df = (
    dedup
    .withColumn("valor", F.col("valorobservado").cast("double"))
    .withColumn("fecha", F.to_timestamp("fechaobservacion", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    .withColumn("latitud", F.col("latitud").cast("double"))
    .withColumn("longitud", F.col("longitud").cast("double"))
)
if UNIFICAR_CODIGOS:
    mapa = F.create_map([F.lit(x) for kv in UNIFICAR_CODIGOS.items() for x in kv])
    df = df.withColumn("codigoestacion", F.coalesce(mapa[F.col("codigoestacion")], F.col("codigoestacion")))
    print(f"[2.2] Codigos unificados (misma estacion fisica): {UNIFICAR_CODIGOS}")
df = df.select("codigoestacion", "nombreestacion", "municipio", "latitud", "longitud", "fecha", "valor")

# 2.3 Nulos o valores no convertibles
n_antes = df.count()
df = df.filter(F.col("valor").isNotNull() & F.col("fecha").isNotNull())
n_nulos = n_antes - df.count()
print(f"[2.2] Filas con valor o fecha no valida: {n_nulos:,}")

# 2.4 Conflictos: misma estacion y fecha con valores distintos
pares = (
    df.groupBy("codigoestacion", "fecha")
    .agg(F.count("*").alias("n"), F.min("valor").alias("vmin"), F.max("valor").alias("vmax"))
    .filter("n > 1")
)
n_pares = pares.count()
n_pares_dif = pares.filter("vmax <> vmin").count()
print(f"[2.3] Pares (estacion, fecha) con mas de una lectura: {n_pares:,} (con valores distintos: {n_pares_dif:,})")
df = df.dropDuplicates(["codigoestacion", "fecha"])
n_unicas = df.count()

# 2.5 Diagnostico de valores bajos, para justificar el rango
print("\n[2.4] Lecturas < 10 C por estacion (diagnostico del limite inferior):")
(df.filter(F.col("valor") < 10)
   .groupBy("nombreestacion")
   .agg(F.count("*").alias("n"), F.min("valor").alias("min"), F.max("valor").alias("max"))
   .orderBy("min")
   .show(30, truncate=False))

# 2.6 Filtro de rango fisico
fuera = df.filter((F.col("valor") < TEMP_MIN) | (F.col("valor") > TEMP_MAX))
n_fuera = fuera.count()
print(f"[2.5] Lecturas fuera del rango [{TEMP_MIN}, {TEMP_MAX}] C: {n_fuera:,}")
if n_fuera:
    fuera.orderBy("valor").show(20, truncate=False)
limpio = df.filter(F.col("valor").between(TEMP_MIN, TEMP_MAX))
n_limpio = limpio.count()

# 2.7 Serie horaria: una fila por estacion y hora (las lecturas fuera de la
#     hora en punto se promedian dentro de su hora)
horario = (
    limpio.withColumn("hora_ts", F.date_trunc("hour", "fecha"))
    .groupBy("codigoestacion", "hora_ts")
    .agg(
        F.avg("valor").alias("temp"),
        F.max("nombreestacion").alias("nombre"),
        F.max("municipio").alias("municipio"),
        F.avg("latitud").alias("latitud"),
        F.avg("longitud").alias("longitud"),
    )
)

# 2.7b Normalizacion de la base horaria (inferida a partir del propio dato)
# Para cada (estacion, dia) con >= 20 horas se correlaciona el ciclo del dia con un
# ciclo de referencia en hora local y con ese mismo ciclo desplazado +DESPLAZ horas.
# Los dias que se parecen mas al desplazado ("U") se llevan a hora local restando
# DESPLAZ horas. Los dias ambiguos heredan la clase del dia clasificado mas cercano
# (+-3 dias) de la misma estacion; si no hay, se dejan como vienen ("?").
print("\n[2.7] Normalizacion de la base horaria (inferida)")
dia_w = Window.partitionBy("codigoestacion", "dia")
base = (
    horario.select("codigoestacion", "nombre", "temp",
                   F.to_date("hora_ts").alias("dia"), F.hour("hora_ts").alias("h"))
    .withColumn("n_h", F.count("*").over(dia_w))
    .filter(F.col("n_h") >= 20)
)
ref_rows = (
    base.filter(F.col("nombre").isin(REF_EST) & F.col("dia").between(REF_INI, REF_FIN))
    .withColumn("anom", F.col("temp") - F.avg("temp").over(dia_w))
    .groupBy("h").agg(F.avg("anom").alias("ref_l"))
    .collect()
)
ref = {r["h"]: r["ref_l"] for r in ref_rows}

if len(ref) == 24:
    h_min = min(ref, key=ref.get)
    h_max = max(ref, key=ref.get)
    print(f"   Ciclo de referencia (hora local): minimo a las {h_min} h, maximo a las {h_max} h")
    refs = spark.createDataFrame(
        [(h, float(ref[h]), float(ref[(h - DESPLAZ) % 24])) for h in range(24)],
        ["h", "ref_l", "ref_u"],
    )
    dias_clase = (
        base.join(F.broadcast(refs), "h")
        .groupBy("codigoestacion", "dia")
        .agg(F.corr("temp", "ref_l").alias("c_l"), F.corr("temp", "ref_u").alias("c_u"))
        .withColumn("c_l", F.when(F.isnan("c_l"), F.lit(None)).otherwise(F.col("c_l")))
        .withColumn("c_u", F.when(F.isnan("c_u"), F.lit(None)).otherwise(F.col("c_u")))
        .withColumn(
            "clase",
            F.when(F.col("c_l") - F.col("c_u") > MARGEN, "L")
             .when(F.col("c_u") - F.col("c_l") > MARGEN, "U"),
        )
    )
    cal = (
        horario.select("codigoestacion", F.to_date("hora_ts").alias("dia")).distinct()
        .join(dias_clase.select("codigoestacion", "dia", "clase"), ["codigoestacion", "dia"], "left")
        .withColumn("dnum", F.datediff("dia", F.lit("2024-01-01").cast("date")))
    )
    w_ant = Window.partitionBy("codigoestacion").orderBy("dnum").rangeBetween(-3, 0)
    w_sig = Window.partitionBy("codigoestacion").orderBy("dnum").rangeBetween(0, 3)
    cal = cal.withColumn(
        "base_horaria",
        F.coalesce(F.col("clase"), F.last("clase", True).over(w_ant),
                   F.first("clase", True).over(w_sig), F.lit("?")),
    ).cache()

    print("   Dias-estacion por mes segun la base horaria inferida (L = local, U = desplazada, ? = sin clasificar):")
    por_mes = (
        cal.withColumn("mes", F.date_format("dia", "yyyy-MM"))
        .groupBy("mes").pivot("base_horaria", ["L", "U", "?"]).count()
        .na.fill(0).orderBy("mes")
    )
    por_mes.show(30, truncate=False)
    por_mes.toPandas().to_csv(os.path.join(SALIDA, "tablas", "base_horaria_por_mes.csv"), index=False)

    horario = (
        horario.withColumn("dia", F.to_date("hora_ts"))
        .join(F.broadcast(cal.select("codigoestacion", "dia", "base_horaria")), ["codigoestacion", "dia"], "left")
        .withColumn(
            "hora_loc",
            F.when(F.col("base_horaria") == "U", F.col("hora_ts") - F.expr(f"INTERVAL {DESPLAZ} HOURS"))
             .otherwise(F.col("hora_ts")),
        )
        .groupBy("codigoestacion", "hora_loc")
        .agg(
            F.avg("temp").alias("temp"),
            F.max("nombre").alias("nombre"),
            F.max("municipio").alias("municipio"),
            F.avg("latitud").alias("latitud"),
            F.avg("longitud").alias("longitud"),
            F.max((F.col("base_horaria") == "U").cast("int")).alias("desplazada"),
        )
        .withColumnRenamed("hora_loc", "hora_ts")
    )
else:
    print("   Sin ciclo de referencia completo: no se normaliza la hora.")
    horario = horario.withColumn("desplazada", F.lit(0))

# 2.8 Etiqueta de estacion: si un mismo nombre tiene varios codigos
#     (p. ej. HACIENDA MANILA) se agregan los 4 ultimos digitos del codigo
w_nombre = Window.partitionBy("nombre")
horario = (
    horario
    .withColumn("n_codigos", F.size(F.collect_set("codigoestacion").over(w_nombre)))
    .withColumn(
        "estacion",
        F.when(
            F.col("n_codigos") > 1,
            F.concat(F.col("nombre"), F.lit(" ["), F.substring("codigoestacion", -4, 4), F.lit("]")),
        ).otherwise(F.col("nombre")),
    )
    .withColumn("anio", F.year("hora_ts"))
    .withColumn("mes", F.month("hora_ts"))
    .withColumn("anio_mes", F.date_format("hora_ts", "yyyy-MM"))
    .withColumn("hora_dia", F.hour("hora_ts"))
    .cache()
)
n_horario = horario.count()

print("\n[2.6] Resumen de la limpieza")
pasos = [
    ("Filas originales", n_raw),
    ("Sin duplicados exactos", n_dedup),
    ("Sin duplicados (estacion, fecha)", n_unicas),
    ("Dentro del rango fisico", n_limpio),
    ("Serie horaria final (hora normalizada)", n_horario),
]
for nombre, n in pasos:
    print(f"   {nombre:<36}{n:>10,}")
print(f"   Retenido: {100 * n_horario / n_raw:.1f}% de las filas originales")

# =============================================================================
# 3. ANALISIS EXPLORATORIO (EDA)
# =============================================================================
titulo("3. ANALISIS EXPLORATORIO (DataFrames)")

print("[3.1] Estadisticos generales de temperatura (C)")
horario.select("temp").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

# Horas posibles en el periodo: base para medir la cobertura de cada estacion
horas_posibles = int(
    horario.agg(
        ((F.max(F.unix_timestamp("hora_ts")) - F.min(F.unix_timestamp("hora_ts"))) / 3600 + 1).alias("h")
    ).first()["h"]
)

print(f"[3.2] Resumen por estacion (horas posibles en el periodo: {horas_posibles:,})")
por_estacion = (
    horario.groupBy("codigoestacion", "estacion", "municipio")
    .agg(
        F.count("*").alias("lecturas"),
        F.round(F.avg("temp"), 2).alias("media"),
        F.round(F.stddev("temp"), 2).alias("desv"),
        F.round(F.min("temp"), 1).alias("min"),
        F.round(F.max("temp"), 1).alias("max"),
        F.round(F.avg("latitud"), 4).alias("lat"),
        F.round(F.avg("longitud"), 4).alias("lon"),
        F.min("hora_ts").alias("primera"),
        F.max("hora_ts").alias("ultima"),
    )
    .withColumn("cobertura_pct", F.round(100 * F.col("lecturas") / F.lit(horas_posibles), 1))
    .orderBy(F.desc("media"))
    .cache()
)
por_estacion.drop("lat", "lon").show(40, truncate=False)

print("[3.3] Evolucion mensual: estaciones activas, lecturas y anomalia")
# Anomalia = lectura menos la media de su propia estacion. Controla el efecto
# de que cada mes reporten estaciones distintas (de distinta altitud).
w_est = Window.partitionBy("codigoestacion")
con_anomalia = horario.withColumn("anomalia", F.col("temp") - F.avg("temp").over(w_est))
mensual = (
    con_anomalia.groupBy("anio_mes")
    .agg(
        F.countDistinct("codigoestacion").alias("estaciones"),
        F.count("*").alias("lecturas"),
        F.round(F.avg("temp"), 2).alias("media_cruda"),
        F.round(F.avg("anomalia"), 2).alias("anomalia_media"),
    )
    .orderBy("anio_mes")
)
mensual.show(30, truncate=False)

print("[3.4] Ciclo diario (promedio por hora del dia, hora local inferida)")
ciclo = (
    horario.groupBy("hora_dia")
    .agg(F.round(F.avg("temp"), 2).alias("media"), F.count("*").alias("lecturas"))
    .orderBy("hora_dia")
)
ciclo.show(24, truncate=False)
_c = ciclo.toPandas()
print(f"   Minimo a las {int(_c.loc[_c['media'].idxmin(), 'hora_dia'])} h, maximo a las {int(_c.loc[_c['media'].idxmax(), 'hora_dia'])} h")

print("[3.5] Diez lecturas horarias mas altas")
(horario.select("estacion", "hora_ts", F.round("temp", 1).alias("temp"))
        .orderBy(F.desc("temp")).show(10, truncate=False))

# =============================================================================
# 4. DEMOSTRACION CON RDD
# =============================================================================
titulo("4. DEMOSTRACION CON RDD (map, filter, reduceByKey, count, take, reduce, collect)")

pares = horario.select("estacion", "temp").rdd.map(lambda r: (r["estacion"], r["temp"]))  # transformacion: map
pares = pares.filter(lambda kv: kv[1] is not None)                                         # transformacion: filter

sumas = (
    pares.map(lambda kv: (kv[0], (kv[1], 1)))
         .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))                             # transformacion: reduceByKey
)
medias = sumas.mapValues(lambda s: s[0] / s[1])

print("Estaciones distintas (count):", medias.count())                                     # accion: count
print("Muestra de 3 pares (take):   ", pares.take(3))                                      # accion: take
suma_total = pares.map(lambda kv: kv[1]).reduce(lambda a, b: a + b)                        # accion: reduce
print(f"Media global con RDD (reduce): {suma_total / n_horario:.3f} C")

medias_rdd = dict(medias.collect())                                                        # accion: collect
medias_df = {r["estacion"]: r["media"] for r in por_estacion.select("estacion", "media").collect()}
coinciden = all(abs(medias_rdd[k] - medias_df[k]) < 0.01 for k in medias_df)
print("Medias por estacion RDD == DataFrame:", coinciden)

# =============================================================================
# 5. VISUALIZACION
# =============================================================================
titulo("5. VISUALIZACION")


def guardar(fig, nombre):
    ruta = os.path.join(SALIDA, "graficos", nombre)
    fig.tight_layout()
    fig.savefig(ruta, dpi=130)
    plt.close(fig)
    print("   guardado:", ruta)


p_est = por_estacion.drop("primera", "ultima").toPandas()
p_men = mensual.toPandas()
p_cic = ciclo.toPandas()

# 5.1 Temperatura media por estacion
p = p_est.sort_values("media")
fig, ax = plt.subplots(figsize=(10, 8))
ax.barh(p["estacion"], p["media"], color="#2a7ab0")
for y, (m, c) in enumerate(zip(p["media"], p["cobertura_pct"])):
    ax.text(m + 0.2, y, f"{m:.1f} C (cobertura {c:.0f}%)", va="center", fontsize=8)
ax.set_xlim(0, p["media"].max() + 9)
ax.set_xlabel("Temperatura media (C)")
ax.set_title("Temperatura media por estacion - Huila 2024-2025")
guardar(fig, "1_media_por_estacion.png")

# 5.2 Cobertura mensual
fig, ax1 = plt.subplots(figsize=(11, 5))
ax1.bar(p_men["anio_mes"], p_men["estaciones"], color="#9ecae1")
ax1.set_ylabel("Estaciones con datos")
ax1.tick_params(axis="x", rotation=60)
ax2 = ax1.twinx()
ax2.plot(p_men["anio_mes"], p_men["lecturas"], color="#d94801", marker="o")
ax2.set_ylabel("Lecturas horarias")
ax1.set_title("Cobertura de datos por mes")
guardar(fig, "2_cobertura_mensual.png")

# 5.3 Anomalia mensual
fig, ax = plt.subplots(figsize=(11, 5))
ax.plot(p_men["anio_mes"], p_men["anomalia_media"], marker="o", color="#b2182b")
ax.axhline(0, color="gray", lw=0.8)
ax.tick_params(axis="x", rotation=60)
ax.set_ylabel("Anomalia media (C) respecto a la media de cada estacion")
ax.set_title("Anomalia mensual de temperatura")
guardar(fig, "3_anomalia_mensual.png")

# 5.4 Ciclo diario
fig, ax = plt.subplots(figsize=(9, 5))
ax.plot(p_cic["hora_dia"], p_cic["media"], marker="o", color="#238b45")
ax.set_xticks(range(0, 24, 2))
ax.set_xlabel("Hora del dia (hora local inferida)")
ax.set_ylabel("Temperatura media (C)")
ax.set_title("Ciclo diario de temperatura")
guardar(fig, "4_ciclo_diario.png")

# 5.5 Mapa de estaciones
fig, ax = plt.subplots(figsize=(8, 8))
sc = ax.scatter(p_est["lon"], p_est["lat"], c=p_est["media"], s=p_est["cobertura_pct"] * 6 + 40,
                cmap="coolwarm", edgecolor="k")
for _, r in p_est.iterrows():
    ax.annotate(r["estacion"], (r["lon"], r["lat"]), fontsize=6, xytext=(3, 3), textcoords="offset points")
fig.colorbar(sc, label="Temperatura media (C)")
ax.set_xlabel("Longitud")
ax.set_ylabel("Latitud")
ax.set_title("Estaciones IDEAM en el Huila (tamano = cobertura)")
guardar(fig, "5_mapa_estaciones.png")

# =============================================================================
# 6. SALIDAS
# =============================================================================
titulo("6. GUARDADO DE RESULTADOS")
p_est.to_csv(os.path.join(SALIDA, "tablas", "resumen_por_estacion.csv"), index=False)
p_men.to_csv(os.path.join(SALIDA, "tablas", "resumen_mensual.csv"), index=False)
p_cic.to_csv(os.path.join(SALIDA, "tablas", "ciclo_diario.csv"), index=False)

horario.drop("n_codigos").write.mode("overwrite").parquet(os.path.join(SALIDA, "datos_limpios_parquet"))

# CSV ordenado por fecha para el productor de Kafka. La fecha se formatea como
# texto en Spark para evitar conversiones de zona horaria al pasar a Python.
(horario
    .select(
        F.col("codigoestacion").alias("sensor_id"),
        F.col("estacion"),
        F.col("municipio"),
        F.round("temp", 2).alias("temperature"),
        F.date_format("hora_ts", "yyyy-MM-dd HH:mm:ss").alias("timestamp"),
    )
    .orderBy("timestamp", "sensor_id")
    .toPandas()
    .to_csv(os.path.join(SALIDA, "ideam_horario_limpio.csv"), index=False))
print("   tablas, parquet y CSV para streaming guardados en:", SALIDA)

titulo("PROCESO BATCH FINALIZADO")
spark.stop()
