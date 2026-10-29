#!/usr/bin/env python3
"""
Tarea 3 - Big Data (UNAD, 202016911)
Productor de Kafka: reenvia las lecturas limpias del IDEAM (Huila) como si
llegaran en tiempo real.

Reemplaza al generador aleatorio del Anexo 3: en vez de inventar datos, reproduce
las lecturas reales (salida del batch) en orden cronologico. Cada "hora simulada"
se envia como un grupo de mensajes (una lectura por estacion con dato en esa hora)
y luego se espera --pausa segundos antes de enviar la hora siguiente.

Uso:
    python3 kafka_producer_ideam.py                      # 72 horas desde 2025-07-01, 1 s por hora
    python3 kafka_producer_ideam.py --horas 24 --pausa 0.5
    python3 kafka_producer_ideam.py --desde "2025-03-10 00:00:00" --horas 48

Cada mensaje es un JSON; la clave del mensaje es el codigo de la estacion, de modo
que todas las lecturas de una estacion van a la misma particion y conservan su orden.
"""
import argparse
import csv
import json
import sys
import time

from kafka import KafkaProducer

ap = argparse.ArgumentParser(description="Productor Kafka con datos del IDEAM")
ap.add_argument("--csv", default="salida/ideam_horario_limpio.csv", help="CSV limpio generado por el batch")
ap.add_argument("--topic", default="ideam_temperatura")
ap.add_argument("--broker", default="localhost:9092")
ap.add_argument("--desde", default="2025-07-01 00:00:00", help="primera hora a reproducir (hora local inferida)")
ap.add_argument("--horas", type=int, default=72, help="cantidad de horas simuladas a enviar")
ap.add_argument("--pausa", type=float, default=1.0, help="segundos reales entre una hora simulada y la siguiente")
args = ap.parse_args()

# ---------------------------------------------------------------- lectura del CSV
grupos = {}  # timestamp -> lista de lecturas de esa hora
try:
    with open(args.csv, newline="", encoding="utf-8") as f:
        for fila in csv.DictReader(f):
            if fila["timestamp"] >= args.desde:   # el formato AAAA-MM-DD HH:MM:SS se ordena como texto
                grupos.setdefault(fila["timestamp"], []).append(fila)
except FileNotFoundError:
    sys.exit(f"No se encuentra {args.csv}. Ejecuta primero el batch (batch_ideam_spark.py).")

horas = sorted(grupos)[: args.horas]
if not horas:
    sys.exit(f"No hay datos desde {args.desde}. Prueba con otra fecha (el dataset cubre 2024-01-01 a 2025-12-31).")

print(f"Archivo: {args.csv}")
print(f"Topic: {args.topic}   Broker: {args.broker}")
print(f"Se enviaran {len(horas)} horas simuladas ({horas[0]} -> {horas[-1]}), "
      f"1 hora cada {args.pausa} s\n")

# ---------------------------------------------------------------- productor
try:
    producer = KafkaProducer(
        bootstrap_servers=[args.broker],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
except Exception as e:  # noqa: BLE001
    sys.exit(f"No se pudo conectar a Kafka en {args.broker}: {e}\n"
             "Verifica con 'jps' que ZooKeeper y Kafka estan corriendo.")

total = 0
estaciones = set()
inicio = time.time()
try:
    for ts in horas:
        lecturas = grupos[ts]
        for fila in lecturas:
            mensaje = {
                "sensor_id": fila["sensor_id"],
                "estacion": fila["estacion"],
                "municipio": fila["municipio"],
                "temperature": float(fila["temperature"]),
                "event_time": fila["timestamp"],   # momento de la medicion (hora local inferida)
                "enviado": round(time.time(), 3),  # momento real del envio
            }
            producer.send(args.topic, key=fila["sensor_id"], value=mensaje)
            estaciones.add(fila["sensor_id"])
        producer.flush()
        total += len(lecturas)
        print(f"[{ts}] enviadas {len(lecturas):>2} lecturas   (acumulado: {total})", flush=True)
        time.sleep(args.pausa)
except KeyboardInterrupt:
    print("\nInterrumpido por el usuario (Ctrl+C).")
finally:
    producer.flush()
    producer.close()

print(f"\nResumen: {total} mensajes, {len(estaciones)} estaciones, "
      f"{time.time() - inicio:.1f} s de ejecucion.")
