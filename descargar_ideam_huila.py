#!/usr/bin/env python3
"""
Descarga de temperatura del aire (IDEAM) desde datos.gov.co
Dataset: Temperatura Ambiente del Aire (ID sbwg-7ju4)
Filtro: departamento HUILA, periodo 2024-2025

Solo usa la libreria estandar de Python 3.

Uso:
    python3 descargar_ideam_huila.py --explorar   # ver columnas y contar filas (no descarga todo)
    python3 descargar_ideam_huila.py              # descarga completa a data/

Opcional: si tienes un App Token de datos.gov.co, exportalo para evitar limites:
    export SODA_APP_TOKEN="tu_token"
"""
import argparse
import csv
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# ---------------- CONFIGURACION (ajustar si el portal cambia) ----------------
DATASET_ID = "sbwg-7ju4"
BASE_URL = f"https://www.datos.gov.co/resource/{DATASET_ID}"

DEPARTAMENTO = "HUILA"
FECHA_INICIO = "2024-01-01T00:00:00"
FECHA_FIN = "2025-12-31T23:59:59"

# Nombres de campo de la API (la API los expone en minusculas).
# Si --explorar muestra nombres distintos, cambialos aqui.
CAMPO_DEPTO = "departamento"
CAMPO_FECHA = "fechaobservacion"

PAGE_SIZE = 50000
SALIDA = os.path.join("data", "ideam_temperatura_huila_2024_2025.csv")
# -----------------------------------------------------------------------------


def _headers():
    h = {"Accept": "text/csv, application/json"}
    token = os.environ.get("SODA_APP_TOKEN")
    if token:
        h["X-App-Token"] = token
    return h


def _get(url, reintentos=4):
    """GET con reintentos y espera creciente."""
    for intento in range(1, reintentos + 1):
        try:
            req = urllib.request.Request(url, headers=_headers())
            with urllib.request.urlopen(req, timeout=120) as resp:
                return resp.read().decode("utf-8")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            print(f"  [intento {intento}/{reintentos}] error: {e}", file=sys.stderr)
            if intento == reintentos:
                raise
            time.sleep(2 * intento)


def clausula_where():
    # upper() para no depender de como venga escrito el departamento
    return (
        f"upper({CAMPO_DEPTO})='{DEPARTAMENTO}' "
        f"AND {CAMPO_FECHA} between '{FECHA_INICIO}' and '{FECHA_FIN}'"
    )


def url(extension, **params):
    return f"{BASE_URL}.{extension}?" + urllib.parse.urlencode(params, safe="(),'")


def explorar():
    print("== 1. Columnas disponibles (5 filas de ejemplo) ==")
    txt = _get(url("json", **{"$limit": 5}))
    filas = json.loads(txt)
    if not filas:
        print("La API no devolvio filas. Revisa el ID del dataset.")
        return
    print("Campos:", ", ".join(filas[0].keys()))
    for f in filas[:3]:
        print(f)

    print("\n== 2. Conteo de filas con el filtro ==")
    txt = _get(url("json", **{"$select": "count(*)", "$where": clausula_where()}))
    total = int(json.loads(txt)[0]["count"])
    print(f"Filas HUILA {FECHA_INICIO[:4]}-{FECHA_FIN[:4]}: {total:,}")
    # Estimacion gruesa: ~120 bytes por fila en CSV
    print(f"Tamano estimado del CSV: ~{total * 120 / 1e6:,.0f} MB")
    if total > 5_000_000:
        print("AVISO: son muchas filas para la VM. Considera recortar el periodo.")

    print("\n== 3. Estaciones distintas ==")
    txt = _get(
        url(
            "json",
            **{
                "$select": "count(distinct codigoestacion) as estaciones",
                "$where": clausula_where(),
            },
        )
    )
    print(json.loads(txt))


def descargar():
    os.makedirs(os.path.dirname(SALIDA), exist_ok=True)
    offset = 0
    total_filas = 0
    escribio_encabezado = False

    with open(SALIDA, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        while True:
            u = url(
                "csv",
                **{
                    "$where": clausula_where(),
                    "$order": ":id",  # orden estable para paginar
                    "$limit": PAGE_SIZE,
                    "$offset": offset,
                },
            )
            print(f"Descargando offset {offset:,} ...", flush=True)
            txt = _get(u)
            lector = csv.reader(io.StringIO(txt))
            encabezado = next(lector, None)
            filas = list(lector)

            if not escribio_encabezado and encabezado:
                writer.writerow(encabezado)
                escribio_encabezado = True
            if not filas:
                break

            writer.writerows(filas)
            total_filas += len(filas)
            offset += PAGE_SIZE
            if len(filas) < PAGE_SIZE:
                break

    print(f"\nListo: {total_filas:,} filas guardadas en {SALIDA}")
    print(f"Tamano: {os.path.getsize(SALIDA) / 1e6:,.1f} MB")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--explorar", action="store_true", help="solo inspeccionar y contar")
    args = ap.parse_args()
    explorar() if args.explorar else descargar()
