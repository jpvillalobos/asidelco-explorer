#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fusiona la hoja 2004.xlsx con los metadatos JSON “enhanced”.

• Clona las columnas originales del Excel.
• Añade nuevas columnas según los JSON (usa 'excel_name' como encabezado).
• Omite la clave 'embedding'.
• Muestra progreso (fila actual / total).
• Registra errores en un log.

Requisitos:
    pip install pandas openpyxl
"""

import os
import json
import time
import logging
import pandas as pd

# ─────────── CONFIGURACIÓN ───────────
SOURCE_EXCEL      = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/input/2024.xlsx"   # ← ajusta la ruta
JSON_DIR          = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/projects/enhanced"
OUTPUT_EXCEL      = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/excell/2004_enhanced.xlsx"
ERROR_LOG_PATH    = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/enrichment_errors.log"

# ─────────── LOGGING ───────────
logging.basicConfig(
    filename=ERROR_LOG_PATH,
    level=logging.ERROR,
    format="%(asctime)s  %(levelname)s  %(message)s",
)

# ─────────── CARGAR EXCEL ORIGINAL ───────────
print("Cargando hoja fuente…")
df = pd.read_excel(SOURCE_EXCEL, engine="openpyxl")
total_rows = len(df)
print(f"Filas encontradas: {total_rows}")

# ─────────── PROCESAMIENTO ───────────
start_time = time.time()

for idx, row in df.iterrows():
    project_id = str(row["Proyecto"]).strip()   # nombre exacto de la columna

    print(f"[{idx + 1}/{total_rows}] Procesando proyecto {project_id}")

    json_path = os.path.join(JSON_DIR, f"{project_id}.json")
    if not os.path.isfile(json_path):
        logging.error(f"JSON no encontrado: {json_path}")
        continue

    try:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        logging.error(f"Error leyendo {json_path}: {exc}")
        continue

    # Recorre cada elemento del JSON
    for key, meta in data.items():
        if key.lower() == "embedding":
            continue  # se ignora

        excel_name = meta.get("excel_name", key).strip()
        value      = meta.get("value")

        # Garantizar que la columna exista
        if excel_name not in df.columns:
            df[excel_name] = pd.NA

        # Escribir el valor en la celda correspondiente
        df.at[idx, excel_name] = value

# ─────────── GUARDAR RESULTADO ───────────
os.makedirs(os.path.dirname(OUTPUT_EXCEL), exist_ok=True)
df.to_excel(OUTPUT_EXCEL, index=False, engine="openpyxl")

elapsed = time.time() - start_time
print(f"\n✅ Proceso completado en {elapsed:,.1f} s")
print(f"Excel enriquecido: {OUTPUT_EXCEL}")
print(f"Log de errores:    {ERROR_LOG_PATH}")
