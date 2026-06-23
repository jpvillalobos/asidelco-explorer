#!/usr/bin/env python3
"""Apply Costa Rica district coordinate cache to a large search-ready JSON file."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple
import argparse
import json
import re
import unicodedata

from services.validation_enrichment_service import ValidationEnrichmentService


SUPPLEMENTAL_DISTRICTS: Dict[str, Dict[str, Any]] = {
    "10707": {
        "codigo": "10707",
        "provincia": "San José",
        "canton": "Mora",
        "distrito": "Quitirrisí",
        "provincia_codigo": "1",
        "canton_codigo": "07",
        "distrito_codigo": "07",
        "latitud": 9.8811276,
        "longitud": -84.2377742,
        "source": "wikipedia_es_current_table",
        "source_title": "Quitirrisí",
        "source_url": "https://es.wikipedia.org/wiki/Quitirrisí",
    },
    "11912": {
        "codigo": "11912",
        "provincia": "San José",
        "canton": "Pérez Zeledón",
        "distrito": "La Amistad",
        "provincia_codigo": "1",
        "canton_codigo": "19",
        "distrito_codigo": "12",
        "latitud": 9.2005523,
        "longitud": -83.5396829,
        "source": "wikipedia_es_current_table",
        "source_title": "La Amistad de Pérez Zeledón",
        "source_url": "https://es.wikipedia.org/wiki/La_Amistad_de_Pérez_Zeledón",
    },
    "21602": {
        "codigo": "21602",
        "provincia": "Alajuela",
        "canton": "Río Cuarto",
        "distrito": "Santa Rita",
        "provincia_codigo": "2",
        "canton_codigo": "16",
        "distrito_codigo": "02",
        "latitud": 10.4040905,
        "longitud": -84.2109776,
        "source": "wikipedia_es_current_table",
        "source_title": "Santa Rita de Río Cuarto",
        "source_url": "https://es.wikipedia.org/wiki/Santa_Rita_de_Río_Cuarto",
    },
    "21603": {
        "codigo": "21603",
        "provincia": "Alajuela",
        "canton": "Río Cuarto",
        "distrito": "Santa Isabel",
        "provincia_codigo": "2",
        "canton_codigo": "16",
        "distrito_codigo": "03",
        "latitud": 10.5035442,
        "longitud": -84.2078502,
        "source": "wikipedia_es_current_table",
        "source_title": "Santa Isabel de Río Cuarto",
        "source_url": "https://es.wikipedia.org/wiki/Santa_Isabel_de_Río_Cuarto",
    },
    "30206": {
        "codigo": "30206",
        "provincia": "Cartago",
        "canton": "Paraíso",
        "distrito": "Birrisito",
        "provincia_codigo": "3",
        "canton_codigo": "02",
        "distrito_codigo": "06",
        "latitud": 9.8596026,
        "longitud": -83.8507902,
        "source": "wikipedia_es_current_table",
        "source_title": "Birrisito",
        "source_url": "https://es.wikipedia.org/wiki/Birrisito",
    },
    "30404": {
        "codigo": "30404",
        "provincia": "Cartago",
        "canton": "Jiménez",
        "distrito": "La Victoria",
        "provincia_codigo": "3",
        "canton_codigo": "04",
        "distrito_codigo": "04",
        "latitud": 9.9177,
        "longitud": -83.7501,
        "source": "wikipedia_es_current_table",
        "source_title": "La Victoria (Costa Rica)",
        "source_url": "https://es.wikipedia.org/wiki/La_Victoria_(Costa_Rica)",
    },
    "50808": {
        "codigo": "50808",
        "provincia": "Guanacaste",
        "canton": "Tilarán",
        "distrito": "Cabeceras",
        "provincia_codigo": "5",
        "canton_codigo": "08",
        "distrito_codigo": "08",
        "latitud": 10.3668225,
        "longitud": -84.8528807,
        "source": "wikipedia_es_current_table",
        "source_title": "Cabeceras",
        "source_url": "https://es.wikipedia.org/wiki/Cabeceras",
    },
    "51105": {
        "codigo": "51105",
        "provincia": "Guanacaste",
        "canton": "Hojancha",
        "distrito": "Matambú",
        "provincia_codigo": "5",
        "canton_codigo": "11",
        "distrito_codigo": "05",
        "latitud": 10.08769,
        "longitud": -85.41952,
        "source": "wikipedia_es_current_table",
        "source_title": "Matambú",
        "source_url": "https://es.wikipedia.org/wiki/Matambú",
    },
    "60206": {
        "codigo": "60206",
        "provincia": "Puntarenas",
        "canton": "Esparza",
        "distrito": "Caldera",
        "provincia_codigo": "6",
        "canton_codigo": "02",
        "distrito_codigo": "06",
        "latitud": 9.9196344,
        "longitud": -84.6966013,
        "source": "wikipedia_es_current_table",
        "source_title": "Caldera (Costa Rica)",
        "source_url": "https://es.wikipedia.org/wiki/Caldera_(Costa_Rica)",
    },
    "60806": {
        "codigo": "60806",
        "provincia": "Puntarenas",
        "canton": "Coto Brus",
        "distrito": "Gutiérrez Braun",
        "provincia_codigo": "6",
        "canton_codigo": "08",
        "distrito_codigo": "06",
        "latitud": 8.9702528,
        "longitud": -82.8775141,
        "source": "wikipedia_es_current_table",
        "source_title": "Gutiérrez Braun",
        "source_url": "https://es.wikipedia.org/wiki/Gutiérrez_Braun",
    },
    "61103": {
        "codigo": "61103",
        "provincia": "Puntarenas",
        "canton": "Garabito",
        "distrito": "Lagunillas",
        "provincia_codigo": "6",
        "canton_codigo": "11",
        "distrito_codigo": "03",
        "latitud": 9.844,
        "longitud": -84.6034,
        "source": "wikipedia_es_current_table",
        "source_title": "Lagunillas (Garabito)",
        "source_url": "https://es.wikipedia.org/wiki/Lagunillas_(Garabito)",
    },
    "70307": {
        "codigo": "70307",
        "provincia": "Limón",
        "canton": "Siquirres",
        "distrito": "Reventazón",
        "provincia_codigo": "7",
        "canton_codigo": "03",
        "distrito_codigo": "07",
        "latitud": 10.2440761,
        "longitud": -83.3873659,
        "source": "wikipedia_es_current_table",
        "source_title": "Reventazón (Costa Rica)",
        "source_url": "https://es.wikipedia.org/wiki/Reventazón_(Costa_Rica)",
    },
}

EXPLICIT_LOOKUP_ALIASES: Dict[str, str] = {
    "cartago|el guarco|tejar": "30801",
    "cartago|cartago|aguacaliente san francisco": "30105",
    "cartago|cartago|guadalupe arenilla": "30106",
    "heredia|san rafael|angeles": "40504",
    "san jose|perez zeledon|general": "11902",
    "san jose|perez zeledon|la amistad": "11912",
    "san jose|moravia|trinidad": "11403",
    "alajuela|upala|san jose pizote": "21303",
    "alajuela|palmares|granja": "20707",
    "alajuela|zarcero|tapezco": "21103",
    "alajuela|rio cuarto|santa rita": "21602",
    "alajuela|rio cuarto|santa isabel": "21603",
    "cartago|paraiso|birrisito": "30206",
    "cartago|jimenez|la victoria": "30404",
    "guanacaste|tilaran|cabeceras": "50808",
    "guanacaste|hojancha|matambu": "51105",
    "puntarenas|esparza|caldera": "60206",
    "puntarenas|coto brus|gutierrez braun": "60806",
    "puntarenas|garabito|lagunillas": "61103",
    "limon|siquirres|reventazon": "70307",
    "san jose|mora|quitirrisi": "10707",
    "san jose|tibas|leon xiii": "11304",
    "puntarenas|monteverde|monteverde": "60109",
    "puntarenas|puerto jimenez|puerto jimenez": "60702",
}


def normalize_key(value: Any) -> str:
    value = "" if value is None else str(value)
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def first_value(record: Dict[str, Any], *fields: str) -> Optional[str]:
    for field in fields:
        value = record.get(field)
        if value not in (None, ""):
            return str(value)
    return None


def admin_lookup_key(record: Dict[str, Any]) -> Tuple[str, str, str, str]:
    province = first_value(record, "project_provincia", "csv_provincia")
    canton = first_value(record, "project_canton", "csv_canton")
    district = first_value(record, "project_distrito", "csv_distrito")
    key = "|".join(normalize_key(value) for value in (province, canton, district))
    return key, province or "", canton or "", district or ""


def load_cache(cache_path: Path) -> Dict[str, Any]:
    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    if "districts" not in cache or "lookup" not in cache:
        raise ValueError(f"Invalid district coordinate cache: {cache_path}")
    cache["districts"].update(SUPPLEMENTAL_DISTRICTS)
    for code, district in SUPPLEMENTAL_DISTRICTS.items():
        key = "|".join(normalize_key(district[field]) for field in ("provincia", "canton", "distrito"))
        cache["lookup"][key] = code
    cache["lookup"].update(EXPLICIT_LOOKUP_ALIASES)
    return cache


def apply_coordinate(record: Dict[str, Any], district: Dict[str, Any]) -> bool:
    lat = district.get("latitud")
    lon = district.get("longitud")
    if lat is None or lon is None:
        return False

    previous_lat = record.get("latitude")
    previous_lon = record.get("longitude")

    record["latitude"] = lat
    record["longitude"] = lon
    record["location"] = {"lat": lat, "lon": lon}
    record["geocoded_address"] = ", ".join(
        value
        for value in [
            district.get("distrito"),
            district.get("canton"),
            district.get("provincia"),
            "Costa Rica",
        ]
        if value
    )
    record["geocoding_level"] = 2
    record["geocoding_description"] = "District reference coordinate"
    record["geocoding_precision"] = "district_reference"
    record["geocoding_source"] = "district_coordinate_cache"
    record["geocoding_status"] = "success"
    record["district_coordinate_cache"] = {
        "codigo": district.get("codigo"),
        "provincia_codigo": district.get("provincia_codigo"),
        "canton_codigo": district.get("canton_codigo"),
        "distrito_codigo": district.get("distrito_codigo"),
        "source": district.get("source"),
        "source_title": district.get("source_title"),
        "source_url": district.get("source_url"),
        "applied_at": datetime.now(timezone.utc).isoformat(),
        "previous_latitude": previous_lat,
        "previous_longitude": previous_lon,
    }
    return True


def iter_records(input_path: Path, chunk_size: int) -> Iterable[Dict[str, Any]]:
    yield from ValidationEnrichmentService()._iter_json_array(input_path, chunk_size=chunk_size)


def repair_file(
    input_path: Path,
    output_path: Path,
    cache_path: Path,
    stats_path: Optional[Path],
    chunk_size: int,
    progress_interval: int,
) -> Dict[str, Any]:
    cache = load_cache(cache_path)
    lookup = cache["lookup"]
    districts = cache["districts"]

    output_path.parent.mkdir(parents=True, exist_ok=True)

    stats: Dict[str, Any] = {
        "input_file": str(input_path),
        "output_file": str(output_path),
        "cache_file": str(cache_path),
        "processed": 0,
        "updated": 0,
        "unchanged_same_coordinate": 0,
        "missing_lookup": 0,
        "missing_coordinate": 0,
        "matched_codes": {},
        "missing_examples": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    with output_path.open("w", encoding="utf-8") as out:
        out.write("[\n")
        first = True
        for record in iter_records(input_path, chunk_size=chunk_size):
            stats["processed"] += 1
            key, province, canton, district_name = admin_lookup_key(record)
            code = lookup.get(key)

            if not code:
                stats["missing_lookup"] += 1
                if len(stats["missing_examples"]) < 25:
                    stats["missing_examples"].append(
                        {
                            "record_id": record.get("record_id"),
                            "lookup_key": key,
                            "provincia": province,
                            "canton": canton,
                            "distrito": district_name,
                        }
                    )
            else:
                district = districts.get(code)
                if not district or district.get("latitud") is None or district.get("longitud") is None:
                    stats["missing_coordinate"] += 1
                else:
                    old_location = record.get("location") or {}
                    old_lat = record.get("latitude", old_location.get("lat"))
                    old_lon = record.get("longitude", old_location.get("lon"))
                    changed = old_lat != district["latitud"] or old_lon != district["longitud"]
                    if apply_coordinate(record, district):
                        stats["matched_codes"][code] = stats["matched_codes"].get(code, 0) + 1
                        if changed:
                            stats["updated"] += 1
                        else:
                            stats["unchanged_same_coordinate"] += 1

            if not first:
                out.write(",\n")
            json.dump(record, out, ensure_ascii=False, separators=(",", ":"))
            first = False

            if progress_interval and stats["processed"] % progress_interval == 0:
                print(
                    f"processed={stats['processed']} updated={stats['updated']} "
                    f"missing_lookup={stats['missing_lookup']}",
                    flush=True,
                )

        out.write("\n]\n")

    stats["finished_at"] = datetime.now(timezone.utc).isoformat()
    stats["matched_distinct_districts"] = len(stats["matched_codes"])

    if stats_path:
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        stats_path.write_text(
            json.dumps(stats, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--cache",
        default=Path("data/reference/costa_rica_district_coordinates_cache.json"),
        type=Path,
    )
    parser.add_argument("--stats", type=Path)
    parser.add_argument("--chunk-size", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--progress-interval", type=int, default=10000)
    args = parser.parse_args()

    stats = repair_file(
        input_path=args.input,
        output_path=args.output,
        cache_path=args.cache,
        stats_path=args.stats,
        chunk_size=args.chunk_size,
        progress_interval=args.progress_interval,
    )
    print(json.dumps(stats, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
