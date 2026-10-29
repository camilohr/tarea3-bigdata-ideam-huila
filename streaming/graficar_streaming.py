#!/usr/bin/env python3
"""
Tarea 3 - Big Data (UNAD, 202016911)
Grafica los resultados que guarda spark_streaming_ideam.py en ./salida_streaming/

Uso:   python3 graficar_streaming.py            (despues de detener el consumidor)
Salida: salida_streaming/graficos/*.png
"""
import glob
import os
import sys

import matplotlib
matplotlib.use("Agg")  # sin entorno grafico: se guardan PNG
import matplotlib.pyplot as plt
import pandas as pd

D = sys.argv[1] if len(sys.argv) > 1 else "salida_streaming"
G = os.path.join(D, "graficos")
os.makedirs(G, exist_ok=True)


def leer(sub):
    """Lee todos los CSV que Spark escribio en la subcarpeta (ignora archivos vacios)."""
    partes = []
    for f in sorted(glob.glob(os.path.join(D, sub, "*.csv"))):
        try:
            x = pd.read_csv(f)
        except pd.errors.EmptyDataError:
            continue
        if len(x):
            partes.append(x)
    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()


def guardar(fig, nombre):
    ruta = os.path.join(G, nombre)
    fig.tight_layout()
    fig.savefig(ruta, dpi=130)
    plt.close(fig)
    print("   guardado:", ruta)


v, a, p = leer("ventanas"), leer("alertas"), leer("particiones")
if v.empty:
    sys.exit(f"No hay resultados en {D}/ventanas. Ejecuta el consumidor y el productor primero.")

# Una misma ventana se actualiza en varios lotes: se conserva su ultima version.
v = v.sort_values("lote").drop_duplicates(["inicio", "estacion"], keep="last")
v["suma"] = v["media"] * v["lecturas"]

# ---------------------------------------------------------------- 1. serie por ventana
g = (v.groupby("inicio")
       .agg(suma=("suma", "sum"), lecturas=("lecturas", "sum"), minimo=("minimo", "min"),
            maximo=("maximo", "max"), estaciones=("estacion", "nunique"))
       .reset_index())
g["media"] = g["suma"] / g["lecturas"]
g["inicio"] = pd.to_datetime(g["inicio"])

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 7), sharex=True, gridspec_kw={"height_ratios": [2, 1]})
ax1.fill_between(g["inicio"], g["minimo"], g["maximo"], color="#9ecae1", alpha=0.5, label="Mínimo a máximo")
ax1.plot(g["inicio"], g["media"], marker="o", color="#08519c", label="Media de las estaciones")
if not a.empty:
    a["event_time"] = pd.to_datetime(a["event_time"])
    ax1.scatter(a["event_time"], a["temperature"], marker="x", color="#cb181d", label="Alertas (temperatura alta)", zorder=3)
ax1.set_ylabel("Temperatura (°C)")
ax1.set_title("Temperatura por ventana de tiempo (datos simulados en tiempo real)")
ax1.legend(loc="upper left", fontsize=8)
ax2.bar(g["inicio"], g["lecturas"], width=0.2, align="edge", color="#6baed6")
ax2.set_ylabel("Lecturas por ventana")
ax2.set_xlabel("Inicio de la ventana (hora del evento)")
ax2b = ax2.twinx()
ax2b.plot(g["inicio"] + pd.Timedelta(hours=3), g["estaciones"], color="#e6550d", marker="s", ms=4)
ax2b.set_ylabel("Estaciones activas", color="#e6550d")
fig.autofmt_xdate()
guardar(fig, "1_ventanas.png")

# ---------------------------------------------------------------- 2. media por estacion
e = v.groupby("estacion").agg(suma=("suma", "sum"), lecturas=("lecturas", "sum"))
e["media"] = e["suma"] / e["lecturas"]
e = e.sort_values("media")
fig, ax = plt.subplots(figsize=(9, max(4, 0.35 * len(e) + 1)))
ax.barh(e.index, e["media"], color="#2a7ab0")
for y, (m, n) in enumerate(zip(e["media"], e["lecturas"])):
    ax.text(m + 0.2, y, f"{m:.1f} °C ({int(n)} lecturas)", va="center", fontsize=8)
ax.set_xlim(0, e["media"].max() + 8)
ax.set_xlabel("Temperatura media (°C)")
ax.set_title("Temperatura media por estación en el periodo procesado")
guardar(fig, "2_media_por_estacion.png")

# ---------------------------------------------------------------- 3. mensajes por particion
if not p.empty:
    pp = p.groupby("particion")["mensajes"].sum().sort_index()
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar([f"Partición {int(i)}" for i in pp.index], pp.values, color="#74a9cf")
    for x, y in enumerate(pp.values):
        ax.text(x, y, str(int(y)), ha="center", va="bottom", fontsize=9)
    ax.set_ylabel("Mensajes consumidos")
    ax.set_title("Mensajes por partición del topic")
    guardar(fig, "3_mensajes_por_particion.png")

# ---------------------------------------------------------------- 4. alertas por estacion
if not a.empty:
    aa = a.groupby("estacion").size().sort_values()
    fig, ax = plt.subplots(figsize=(8, max(3, 0.4 * len(aa) + 1)))
    ax.barh(aa.index, aa.values, color="#de2d26")
    ax.set_xlabel("Cantidad de alertas")
    ax.set_title("Alertas de temperatura alta por estación")
    guardar(fig, "4_alertas_por_estacion.png")

print(f"\nResumen: {int(v['lecturas'].sum())} lecturas en {g.shape[0]} ventanas y {e.shape[0]} estaciones; "
      f"{0 if a.empty else len(a)} alertas.")
