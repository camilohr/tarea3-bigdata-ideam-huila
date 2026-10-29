# Tarea 3 – Big Data (UNAD 202016911): procesamiento con Apache Spark

Análisis con PySpark de la temperatura del aire a 2 m registrada por las estaciones
automáticas del IDEAM en el departamento del Huila (2024-2025), con una simulación en
tiempo real usando Kafka y Spark Structured Streaming.

## Datos
- Fuente: IDEAM, conjunto «Temperatura Ambiente del Aire», Datos Abiertos Colombia
  (ID `sbwg-7ju4`): https://www.datos.gov.co/Ambiente-y-Desarrollo-Sostenible/Temperatura-Ambiente-del-Aire/sbwg-7ju4
- Licencia de los datos: CC BY-SA 4.0. Los datos no se incluyen en este repositorio:
  se descargan con el script.
- Recorte: departamento HUILA, 2024-01-01 a 2025-12-31 (240.564 registros, 12 campos).

## Contenido
- `descargar_ideam_huila.py`: descarga el recorte desde la API del portal.
- `batch_ideam_spark.py`: procesamiento batch (limpieza, transformación, EDA, RDD, gráficos).
- `streaming/`: productor de Kafka, consumidor de Spark Streaming, gráficos y script de arranque.

## Requisitos
Probado en Ubuntu 22.04 con Java 11, Apache Spark 3.5.8 (Scala 2.12), Python 3.10,
pandas y matplotlib.

## Ejecución del batch
```bash
python3 descargar_ideam_huila.py --explorar   # revisa columnas y número de filas
python3 descargar_ideam_huila.py              # crea data/ideam_temperatura_huila_2024_2025.csv
spark-submit --driver-memory 1g batch_ideam_spark.py
```
Los resultados quedan en `salida/` (gráficos, tablas, datos limpios en Parquet y un CSV
ordenado por fecha para el productor de Kafka).

## Qué hace el batch
1. Carga el CSV y elimina duplicados exactos (45.150 filas).
2. Convierte tipos, unifica dos códigos de la misma estación y filtra lecturas fuera de 5-45 °C.
3. Construye una serie horaria por estación y normaliza la base horaria (ver limitaciones).
4. Análisis exploratorio con DataFrames y una demostración con RDD.
5. Genera 5 gráficos: media por estación, cobertura mensual, anomalía mensual, ciclo diario y mapa.

## Limitaciones
- Desde el 29/10/2024 la mayoría de las estaciones muestra el ciclo diario desplazado 5 h
  (coherente con UTC). La corrección es una inferencia a partir de los datos, no está
  confirmada por el IDEAM.
- La cobertura es irregular: solo 4 estaciones superan el 89 % de las horas posibles.

## Streaming (Kafka + Spark Streaming)
```bash
bash streaming/iniciar_kafka.sh               # ZooKeeper, Kafka y topic de 3 particiones
spark-submit --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 streaming/spark_streaming_ideam.py
python3 streaming/kafka_producer_ideam.py     # en otra terminal
python3 streaming/graficar_streaming.py       # al terminar
```
El productor reproduce las lecturas limpias (`salida/ideam_horario_limpio.csv`) a una hora
simulada por segundo.

## Autoría
Jean Harvy Vasquez Montealegre, Universidad Nacional Abierta y a Distancia, octubre de 2026.
