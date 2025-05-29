#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CFIA-Explorer Scraper
──────────────────────────────────────────────────────────────
• Lee proyectos HTML, extrae el carnet profesional
• Consulta listado de miembros CFIA (POST)
• Encuentra la URL de detalle del profesional en el HTML devuelto
  – Patrón tolerante a comillas simples/dobles, ‘\’, punto-y-coma opcional
• Descarga y guarda HTML + JSON de detalle
• Registra progreso, carnets procesados y errores
"""

import os
import json
import re
import random
import time
import requests
from time import sleep, strftime
from datetime import datetime
from bs4 import BeautifulSoup
from html_to_json_parser import parse_project_html_file   # tu parser local

# ─────────────────────── RUTAS ───────────────────────
BASE = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output"

DIR_IN_PROY        = f"{BASE}/projects/html"
DIR_OUT_PROY_JSON  = f"{BASE}/projects/json_5"
DIR_OUT_PROF_HTML  = f"{BASE}/professionals_5/html"
DIR_OUT_PROF_JSON  = f"{BASE}/professionals_5/json"

LOG_INPUT   = f"{BASE}/journey_inputfiles_5.log"
LOG_CARNETS = f"{BASE}/journey_carnets_5.log"
LOG_ERROR   = f"{BASE}/journey_professionals_errors_5.log"

for p in (DIR_OUT_PROY_JSON, DIR_OUT_PROF_HTML, DIR_OUT_PROF_JSON):
    os.makedirs(p, exist_ok=True)

# ─────────────────────── HTTP ───────────────────────
BASE_URL     = "https://servicios.cfia.or.cr"
URL_MIEMBROS = f"{BASE_URL}/ListadoMiembros/Miembros/"

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": BASE_URL,
    "referer": URL_MIEMBROS,
}

session = requests.Session()

# ─────────────────────── UTILS ───────────────────────
def log(path: str, msg: str) -> None:
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(msg + "\n")

def load_set(path: str, pattern: str | None = None) -> set[str]:
    """Carga un archivo de log y devuelve el conjunto de valores únicos."""
    if not os.path.exists(path):
        return set()
    s: set[str] = set()
    for line in open(path, encoding="utf-8"):
        if pattern:
            m = re.search(pattern, line)
            if m:
                s.add(m.group(1))
        else:
            parts = line.split()
            if parts:
                s.add(parts[0])
    return s

def first_carnet(texto: str) -> str:
    """Devuelve el primer carnet cuando vienen separados por coma."""
    return texto.split(",")[0].strip()

# ─────────────────────── ESTADO ───────────────────────
done_files   = load_set(LOG_INPUT)
done_carnets = load_set(LOG_CARNETS, r"Processed carnet: (\S+)")

FILES = sorted(
    f for f in os.listdir(DIR_IN_PROY)
    if f.endswith(".html") and f not in done_files
)

total = len(FILES)
start = time.time()

# ─────────────────────── PROCESO PRINCIPAL ───────────────────────
for idx, fname in enumerate(FILES, 1):
    elapsed = time.time() - start
    h, rem = divmod(int(elapsed), 3600)
    m, s   = divmod(rem, 60)
    print(f"[{idx}/{total}] {fname} | {h:02}:{m:02}:{s:02}")

    # ── Parsear proyecto ──
    proj_html_path = os.path.join(DIR_IN_PROY, fname)
    proj = parse_project_html_file(proj_html_path)
    if not proj:
        msg = f"{fname} {strftime('%F %T')} - HTML inválido"
        log(LOG_ERROR, msg); log(LOG_INPUT, msg)
        continue

    json.dump(
        proj,
        open(os.path.join(DIR_OUT_PROY_JSON, fname.replace(".html", ".json")), "w", encoding="utf-8"),
        ensure_ascii=False, indent=2
    )

    carnet_raw = proj.get("Carnet Profesional", "").strip()
    if not carnet_raw:
        msg = f"{fname} {strftime('%F %T')} - Sin campo Carnet"
        log(LOG_ERROR, msg); log(LOG_INPUT, msg)
        continue

    carnet = first_carnet(carnet_raw)
    if carnet in done_carnets:
        print(f" • {carnet} ya procesado, salto.")
        continue

    # ── POST listado de miembros ──
    payload = {
        "Consulta.CheckFiltro": "1",
        "Consulta.Dato": carnet,
        "Consulta.ColegioCiviles": "true",
        "Consulta.ColegioArquitectos": "true",
        "Consulta.ColegioCiemi": "true",
        "Consulta.ColegioTopografos": "true",
        "Consulta.ColegioTecnologos": "true",
        "Consulta.Provincia": "0",
        "Consulta.Canton": "0",
        "Consulta.Distrito": "0",
    }
    try:
        r_list = session.post(URL_MIEMBROS, headers=HEADERS, data=payload, timeout=15)
        r_list.raise_for_status()
    except Exception as e:
        msg = f"{fname} {strftime('%F %T')} - POST error {carnet}: {e}"
        log(LOG_ERROR, msg); log(LOG_INPUT, msg); continue

    html_list = r_list.text
    open(os.path.join(DIR_OUT_PROF_HTML, f"{carnet}.html"), "w", encoding="utf-8").write(html_list)

    # ── Patrón exacto: URL detalle + fila con carnet ──
    pat_exact = re.compile(
        rf"""
            var\s*elemento\s*=\s*          # var elemento=
            \\?["']                        #   comilla opcionalmente escapada
            (?P<path>/ListadoMiembros/Miembros/DetalleMiembro\?cedula=\d+)
            ["']\s*;?                      #   comilla cierre y ; opcional
            .*?                            #   cualquier cosa (no codiciosa)
            <td\s+class=\\?['"]tablaMiembros\\?['"]>
            \s*{re.escape(carnet)}\s*
            </td>
        """,
        flags=re.IGNORECASE | re.DOTALL | re.VERBOSE
    )

    m = pat_exact.search(html_list)

    if not m:
        # ── Fallback: primera variable elemento ──
        m = re.search(
            r"var\s*elemento\s*=\s*\\?['\"](?P<path>/ListadoMiembros/Miembros/DetalleMiembro\?cedula=\d+)['\"]",
            html_list,
            flags=re.IGNORECASE
        )
        if not m:
            msg = f"{fname} {strftime('%F %T')} - Sin link detalle {carnet}"
            log(LOG_ERROR, msg); log(LOG_INPUT, msg); continue
        detail_path = m.group("path").replace("\\/", "/")
        print("   link exacto no hallado; usando primer match")
    else:
        detail_path = m.group("path").replace("\\/", "/")
        print("   link exacto encontrado")

    # ── GET detalle ──
    try:
        r_det = session.get(BASE_URL + detail_path, headers=HEADERS, timeout=15)
        r_det.raise_for_status()
    except Exception as e:
        msg = f"{fname} {strftime('%F %T')} - GET detalle {carnet}: {e}"
        log(LOG_ERROR, msg); log(LOG_INPUT, msg); continue

    html_det = r_det.text
    open(os.path.join(DIR_OUT_PROF_HTML, f"{carnet}-detail.html"), "w", encoding="utf-8").write(html_det)

    # ── Parseo detalle ──
    soup = BeautifulSoup(html_det, "html.parser")
    sec  = soup.select_one("section.container.documentsPage.seccionBuscador")
    if sec:
        inputs = sec.find_all(["input", "textarea"])
        det_json = {t.get("name"): t.get("value", t.text.strip()) for t in inputs if t.get("name")}
        det_json["project_id"] = carnet
        json.dump(
            det_json,
            open(os.path.join(DIR_OUT_PROF_JSON, f"{carnet}-detail.json"), "w", encoding="utf-8"),
            ensure_ascii=False, indent=2
        )
    else:
        log(LOG_ERROR, f"{fname} {strftime('%F %T')} - Sin sección detalle {carnet}")

    done_carnets.add(carnet)
    log(LOG_CARNETS, f"{datetime.now().isoformat()} - Processed carnet: {carnet} from file: {fname}")
    log(LOG_INPUT,   f"{fname} {strftime('%F %T')} - Processed")

    delay = random.uniform(0, 0.5)
    print(f"   espera {delay:.2f}s …")
    sleep(delay)

# ─────────────────────── RESUMEN FINAL ───────────────────────
total_elapsed = time.time() - start
h, rem = divmod(int(total_elapsed), 3600)
m, s  = divmod(rem, 60)
print(f"\nFin: {h}h {m}m {s}s.")
