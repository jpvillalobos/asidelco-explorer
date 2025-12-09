#!/usr/bin/env python3
"""
Enriquecedor CFIA-Explorer
──────────────────────────────────────────────────────────────
• Normaliza fechas y montos
• Maneja carnets múltiples ("ICO-123, IME-456") y toma el primer match
• Aplana la información del profesional en cada proyecto
• Genera un resumen en español + embedding (SBERT)
• Añade geolocalización (Nominatim) con caché local
• Renombra a snake_case ES
• Exporta:
      "campo": {
          "value":        …,
          "display_name": "<nombre JSON original>",
          "excel_name":   "<encabezado Excel>"
      }
• Muestra tiempo acumulado en cada línea de progreso
"""

import os
import json
import time
import unicodedata
import requests
from datetime import datetime

# ─────────── Embeddings (opcional) ───────────
try:
    from sentence_transformers import SentenceTransformer
    MODEL = SentenceTransformer("all-MiniLM-L6-v2")
except ModuleNotFoundError:
    print("[INFO] sentence_transformers no instalado → embeddings vacíos.")
    MODEL = None

# ─────────── Rutas ───────────
BASE = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output"
DIR_PROYECTOS     = f"{BASE}/projects/json"
DIR_PROFESIONALES = f"{BASE}/professionals/json"
DIR_SALIDA        = f"{BASE}/projects/enhanced"
LOG_ERRORES       = f"{BASE}/enhancements-errors.log"
CACHE_GEO         = f"{BASE}/geolocation_cache.json"

os.makedirs(DIR_SALIDA, exist_ok=True)

# ─────────── Cargar caché geo ───────────
GEO_CACHE = (
    json.load(open(CACHE_GEO, encoding="utf-8"))
    if os.path.exists(CACHE_GEO)
    else {}
)

# ─────────── Constantes ───────────
CAMPOS_FECHA = ["Fecha Proyecto", "Fecha de ingreso trámite municipal"]
CAMPOS_MONTO = ["Tasado", "Monto Pendiente de pago"]

MAPEO_PROFESIONAL = {
    "Cedula":           "Cedula del profesional",
    "Carne":            "Carne del profesional",
    "NombreCompleto":   "Nombre del profesional",
    "Colegio":          "Colegio del profesional",
    "CorreoPermanente": "Correo permanente del profesional",
    "CorreoLaboral":    "Correo Laboral del profesional",
    "Condicion":        "Condicion del profesional",
    "TelCelular":       "Telefono celular del profesional",
    "TelOficina":       "Telefono de oficina del profesional",
    "Fax":              "Fax del profesional",
    "Lugar":            "Lugar del profesional",
    "Direccion":        "Direccion del profesional",
}

RENOMBRA = {
    "Num. Proyecto":"numero_proyecto",
    "Fecha Proyecto":"fecha_proyecto",
    "Estado":"estado_proyecto",
    "Detalle de Estado":"detalle_estado",
    "Cédula Propietario":"cedula_propietario",
    "Nombre Propietario":"nombre_propietario",
    "Num. APC":"numero_apc",
    "Num. Boleta":"numero_boleta",
    "Estado de la Boleta":"estado_boleta",
    "Catastro":"codigo_catastro",
    "Descripción del proyecto":"descripcion_proyecto",
    "Clasificación":"clasificacion",
    "Dirección Exacta":"direccion_exacta",
    "Provincia":"provincia",
    "Cantón":"canton",
    "Distrito":"distrito",
    "Carnet Profesional":"carnet_profesional",
    "Carnet Empresa":"carnet_empresa",
    "Responsable":"empresa_responsable",
    "Descripción":"detalle_proyecto",
    "Tasado":"valor_tasado",
    "Constancia de Recibido":"constancia_recibido",
    "Fecha de ingreso trámite municipal":"fecha_ingreso_municipal",
    "Cantidad de días en cola de revisión de la Municipalidad":"dias_en_cola_municipal",
    "Monto Pendiente de pago":"monto_pendiente_pago",
    "Pagar Proyecto":"metodo_pago",
    "Detalle":"detalle_pago",
    "Cedula del profesional":"cedula_profesional",
    "Carne del profesional":"carnet_profesional_dup",
    "Nombre del profesional":"nombre_profesional",
    "Colegio del profesional":"colegio_profesional",
    "Correo permanente del profesional":"email_profesional_personal",
    "Correo Laboral del profesional":"email_profesional_laboral",
    "Condicion del profesional":"condicion_profesional",
    "Telefono celular del profesional":"telefono_profesional_movil",
    "Telefono de oficina del profesional":"telefono_profesional_oficina",
    "Fax del profesional":"fax_profesional",
    "Lugar del profesional":"lugar_profesional",
    "Direccion del profesional":"direccion_profesional",
    # Campos generados
    "summary":"resumen",
    "geolocation_lat":"latitud",
    "geolocation_lon":"longitud",
    "embedding":"embedding",
}

# ─────────── Utils ───────────
def log_error(msg: str) -> None:
    with open(LOG_ERRORES, "a", encoding="utf-8") as fh:
        fh.write(f"[{time.strftime('%F %T')}] {msg}\n")


def quitar_acentos(txt: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize("NFD", txt)
        if unicodedata.category(c) != "Mn"
    )


def excel_name(snake: str) -> str:
    return quitar_acentos(snake.replace("_", " ").title())


def normalizar_fecha(txt: str) -> str:
    if not txt:
        return ""
    formatos = (
        "%m/%d/%Y %I:%M:%S %p",
        "%d/%m/%Y %I:%M:%S %p",
        "%m/%d/%Y",
        "%d/%m/%Y",
    )
    for fmt in formatos:
        try:
            dt = datetime.strptime(txt, fmt)
            if ":%S" not in fmt:
                dt = dt.replace(hour=0, minute=0, second=0)
            return dt.strftime("%d/%m/%Y %H:%M:%S")
        except ValueError:
            continue
    return txt


def normalizar_float(v):
    if isinstance(v, str):
        v = v.replace(",", "")
    try:
        return float(v)
    except Exception:
        return 0.0


def geolocalizar(prov: str, cant: str, dist: str) -> dict:
    if not (prov and cant and dist):
        return {}
    key = f"{prov}::{cant}::{dist}"
    if key in GEO_CACHE:
        return GEO_CACHE[key]

    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": f"{dist}, {cant}, {prov}, Costa Rica",
                "format": "json",
                "countrycodes": "cr",
                "limit": 1,
            },
            headers={"User-Agent": "cfia-explorer/1.0"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data:
            GEO_CACHE[key] = {
                "lat": float(data[0]["lat"]),
                "lon": float(data[0]["lon"]),
            }
        time.sleep(1)  # cortesía para Nominatim
    except Exception as exc:
        log_error(f"Geo {key}: {exc}")
    return GEO_CACHE.get(key, {})


def resumen_es(proy: dict, prof: dict) -> str:
    n, clas = proy.get("Num. Proyecto", ""), proy.get("Clasificación", "").lower()
    t = proy.get("Descripción del proyecto", "").capitalize()
    dist, cant, prov = (proy.get(k, "") for k in ("Distrito", "Cantón", "Provincia"))
    est, det = proy.get("Estado", ""), proy.get("Detalle de Estado", "")
    fecha = proy.get("Fecha Proyecto") or proy.get(
        "Fecha de ingreso trámite municipal", ""
    )
    tas = proy.get("Tasado", "")
    valor = f"₡{tas:,.0f}" if isinstance(tas, (int, float)) else tas
    prop = proy.get("Nombre Propietario", "")
    nom = prof.get("NombreCompleto", "")
    car = prof.get("Carne", "")
    col = prof.get("Colegio", "")
    emp = proy.get("Responsable", "")
    mail = prof.get("CorreoLaboral") or prof.get("CorreoPermanente", "")
    tel = prof.get("TelCelular") or prof.get("TelOficina", "")
    return (
        f"El proyecto {n} ({clas}) titulado \"{t}\" se ubica en "
        f"{dist}, {cant}, {prov}. Estado: {est} – {det}. Valor tasado {valor}, "
        f"fecha {fecha}. Propietario: {prop}. Profesional: {nom} ({car}, {col}). "
        f"Empresa: {emp}. Contacto: {mail}, {tel}."
    )


# ─────────── Carga de archivos ───────────
def cargar_jsones(path: str) -> dict:
    data = {}
    for fname in os.listdir(path):
        if not fname.endswith(".json"):
            continue
        try:
            j = json.load(open(os.path.join(path, fname), encoding="utf-8"))
            key = j.get("project_id") or j.get("Num. Proyecto") or j.get("Carne")
            if key:
                data[key.strip()] = j
        except Exception as exc:
            log_error(f"{fname}: {exc}")
    return data


# Profesionales: indexa todos los carnets (divididos por coma)
PROFES = {}
for p in cargar_jsones(DIR_PROFESIONALES).values():
    for token in [t.strip() for t in p.get("Carne", "").split(",") if t.strip()]:
        PROFES.setdefault(token, p)  # conserva el primero

# Proyectos
PROYECTOS = cargar_jsones(DIR_PROYECTOS)

# ─────────── Procesamiento ───────────
t0_global = time.time()
total = len(PROYECTOS)

for idx, (pid, proj) in enumerate(sorted(PROYECTOS.items()), 1):
    print(f"[{idx}/{total}] {pid} | tiempo acumulado: {time.time()-t0_global:.1f}s")

    carnet_raw = proj.get("Carnet Profesional", "").strip()
    if not carnet_raw:
        log_error(f"{pid} sin carnet"); continue

    prof = None
    for token in [t.strip() for t in carnet_raw.split(",") if t.strip()]:
        prof = PROFES.get(token)
        if prof:
            break
    if not prof:
        log_error(f"{pid} sin profesional"); continue

    # Normalizaciones
    for campo in CAMPOS_MONTO:
        proj[campo] = normalizar_float(proj.get(campo))
    for campo in CAMPOS_FECHA:
        proj[campo] = normalizar_fecha(proj.get(campo))

    # Aplanar profesional
    for src, dst in MAPEO_PROFESIONAL.items():
        proj[dst] = prof.get(src, "")

    # Resumen + embedding
    proj["summary"] = resumen_es(proj, prof)
    proj["embedding"] = (
        MODEL.encode([proj["summary"]])[0].tolist() if MODEL else []
    )

    # Geolocalización
    coords = geolocalizar(
        proj.get("Provincia", ""), proj.get("Cantón", ""), proj.get("Distrito", "")
    )
    proj["geolocation_lat"], proj["geolocation_lon"] = (
        coords.get("lat", ""),
        coords.get("lon", ""),
    )

    # Construir salida con tres nombres
    salida = {}
    for original, value in proj.items():
        clave = RENOMBRA.get(original, original)
        salida[clave] = {
            "value": value,
            "display_name": original,
            "excel_name": excel_name(clave),
        }

    try:
        json.dump(
            salida,
            open(os.path.join(DIR_SALIDA, f"{pid}.json"), "w", encoding="utf-8"),
            ensure_ascii=False,
            indent=2,
        )
    except Exception as exc:
        log_error(f"{pid} guardar: {exc}")

# Guardar caché geo
json.dump(GEO_CACHE, open(CACHE_GEO, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

print(f"\nProcesados {total} proyectos en {time.time()-t0_global:.1f}s")
