#!/usr/bin/env python3
"""Build Costa Rica district coordinate cache from official hierarchy + Wikipedia.

The administrative hierarchy comes from the JSON source used as the source of
truth for province/canton/district codes. Wikipedia is used only to resolve each
district page and its published coordinate metadata.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
import json
import re
import time
import unicodedata
import urllib.parse

import requests
from bs4 import BeautifulSoup


ADMIN_SOURCE_URL = (
    "https://gist.githubusercontent.com/josuenoel/80daca657b71bc1cfd95a4e27d547abe/"
    "raw/5c615419196ed40a3dbdff69cb3d9719b1d6bb1e/"
    "provincias_cantones_distritos_costa_rica.json"
)
WIKI_DISTRICTS_URL = "https://es.wikipedia.org/wiki/Anexo:Distritos_de_Costa_Rica"
WIKI_API_URL = "https://es.wikipedia.org/w/api.php"
WIKI_PAGE_BASE_URL = "https://es.wikipedia.org/wiki/"
USER_AGENT = "asidelco-explorer-district-coordinate-cache/1.0"

MANUAL_COORDINATE_OVERRIDES: Dict[str, Dict[str, Any]] = {
    # The hierarchy source still has Rio Cuarto as Grecia district 20306. It was
    # later reorganized into Río Cuarto canton/district. The current Wikipedia
    # district page is linked under code 21601, so the old code is not present in
    # the current district list table.
    "20306": {
        "latitud": 10.412222,
        "longitud": -84.215556,
        "source": "wikipedia_es_manual_fallback",
        "source_url": "https://es.wikipedia.org/wiki/Cantón_de_Río_Cuarto",
        "source_title": "Cantón de Río Cuarto",
        "source_note": "Older hierarchy code for Rio Cuarto under Grecia; coordinates sourced from the current Río Cuarto canton page.",
    },
    # The Spanish Tres Ríos district page currently has no coordinate metadata in
    # the API, but the equivalent English page does.
    "30301": {
        "latitud": 9.907172,
        "longitud": -83.9864611,
        "source": "wikipedia_en_manual_fallback",
        "source_url": "https://en.wikipedia.org/wiki/Tres_Ríos,_Cartago",
        "source_title": "Tres Ríos, Cartago",
        "source_note": "Spanish page lacks API coordinate metadata; coordinates sourced from the English district page.",
    },
}


def normalize_key(value: str) -> str:
    """Normalize admin names for cache lookup."""
    value = unicodedata.normalize("NFKD", value or "")
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def iter_admin_districts(admin_data: Dict[str, Any]) -> Iterable[Dict[str, str]]:
    provincias = admin_data["provincias"]
    for province_code, province in provincias.items():
        for canton_code, canton in province["cantones"].items():
            for district_code, district_name in canton["distritos"].items():
                code = f"{province_code}{canton_code}{district_code}"
                yield {
                    "codigo": code,
                    "provincia_codigo": province_code,
                    "canton_codigo": canton_code,
                    "distrito_codigo": district_code,
                    "provincia": province["nombre"],
                    "canton": canton["nombre"],
                    "distrito": district_name,
                }


def fetch_json(session: requests.Session, url: str) -> Dict[str, Any]:
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


def fetch_district_page_links(session: requests.Session) -> Dict[str, Dict[str, str]]:
    """Return mapping from district code/postal code to linked Wikipedia page."""
    response = session.get(WIKI_DISTRICTS_URL, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="wikitable")
    if table is None:
        raise RuntimeError("Could not locate district table on Wikipedia page")

    links: Dict[str, Dict[str, str]] = {}
    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        texts = [cell.get_text(" ", strip=True) for cell in cells]
        code_index = next(
            (idx for idx, text in enumerate(texts) if re.fullmatch(r"\d{5}", text)),
            None,
        )
        if code_index is None or code_index == 0:
            continue

        district_cell = cells[code_index - 1]
        district_link = district_cell.find("a", href=True)
        if not district_link or "/wiki/" not in district_link["href"]:
            continue

        code = texts[code_index]
        page_name = district_link["href"].split("/wiki/", 1)[1]
        page_name = urllib.parse.unquote(page_name)
        links[code] = {
            "table_district": district_link.get_text(" ", strip=True),
            "wiki_page_name": page_name,
            "source_url": WIKI_PAGE_BASE_URL + urllib.parse.quote(page_name, safe="()_"),
        }

    return links


def chunks(values: List[Tuple[str, str]], size: int) -> Iterable[List[Tuple[str, str]]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def fetch_coordinates(
    session: requests.Session,
    code_to_page: Dict[str, Dict[str, str]],
    batch_size: int = 50,
) -> Dict[str, Dict[str, Any]]:
    """Fetch coordinates from the MediaWiki API in batches."""
    items = [(code, page["wiki_page_name"]) for code, page in code_to_page.items()]
    coordinates: Dict[str, Dict[str, Any]] = {}

    for batch in chunks(items, batch_size):
        titles = [page_name for _, page_name in batch]
        response = session.get(
            WIKI_API_URL,
            params={
                "action": "query",
                "format": "json",
                "prop": "coordinates",
                "colimit": "max",
                "redirects": 1,
                "titles": "|".join(titles),
            },
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json().get("query", {})

        normalized = {
            item["from"]: item["to"]
            for item in payload.get("normalized", [])
        }
        redirects = {
            item["from"]: item["to"]
            for item in payload.get("redirects", [])
        }
        pages_by_title = {
            page.get("title"): page
            for page in payload.get("pages", {}).values()
        }

        for code, page_name in batch:
            title = normalized.get(page_name, page_name.replace("_", " "))
            title = redirects.get(title, title)
            page = pages_by_title.get(title)
            if not page:
                coordinates[code] = {"missing_reason": "page_not_returned"}
                continue

            page_coordinates = page.get("coordinates") or []
            if not page_coordinates:
                coordinates[code] = {
                    "source_title": page.get("title"),
                    "source_pageid": page.get("pageid"),
                    "missing_reason": "coordinates_not_found",
                }
                continue

            primary = page_coordinates[0]
            coordinates[code] = {
                "latitud": primary["lat"],
                "longitud": primary["lon"],
                "coordinate_globe": primary.get("globe", "earth"),
                "source_title": page.get("title"),
                "source_pageid": page.get("pageid"),
            }

        time.sleep(0.1)

    return coordinates


def lookup_keys(record: Dict[str, str]) -> List[str]:
    province = normalize_key(record["provincia"])
    canton = normalize_key(record["canton"])
    district = normalize_key(record["distrito"])
    keys = [f"{province}|{canton}|{district}"]

    if canton == "central":
        keys.append(f"{province}|{province}|{district}")

    return sorted(set(keys))


def build_cache() -> Dict[str, Any]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    admin_data = fetch_json(session, ADMIN_SOURCE_URL)
    districts = list(iter_admin_districts(admin_data))
    wiki_links = fetch_district_page_links(session)
    coordinates = fetch_coordinates(session, wiki_links)

    records: Dict[str, Dict[str, Any]] = {}
    lookup: Dict[str, str] = {}
    missing: List[Dict[str, Any]] = []

    for district in districts:
        code = district["codigo"]
        link = wiki_links.get(code, {})
        coordinate = coordinates.get(code, {})

        record: Dict[str, Any] = {
            **district,
            "lookup_keys": lookup_keys(district),
            "source": "wikipedia_es",
            "source_url": link.get("source_url"),
            "source_title": coordinate.get("source_title"),
            "source_pageid": coordinate.get("source_pageid"),
        }

        override = MANUAL_COORDINATE_OVERRIDES.get(code)
        if override:
            record.update(override)
            record["geo_point"] = {
                "lat": override["latitud"],
                "lon": override["longitud"],
            }
            record["coordinate_globe"] = "earth"
        elif "latitud" in coordinate and "longitud" in coordinate:
            record.update(
                {
                    "latitud": coordinate["latitud"],
                    "longitud": coordinate["longitud"],
                    "geo_point": {
                        "lat": coordinate["latitud"],
                        "lon": coordinate["longitud"],
                    },
                    "coordinate_globe": coordinate.get("coordinate_globe", "earth"),
                }
            )
        else:
            record["missing_reason"] = coordinate.get(
                "missing_reason",
                "wikipedia_link_not_found" if code not in wiki_links else "coordinates_not_found",
            )
            missing.append(
                {
                    "codigo": code,
                    "provincia": district["provincia"],
                    "canton": district["canton"],
                    "distrito": district["distrito"],
                    "reason": record["missing_reason"],
                    "source_url": record["source_url"],
                }
            )

        records[code] = record
        for key in record["lookup_keys"]:
            lookup[key] = code

    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "admin_source_url": ADMIN_SOURCE_URL,
            "coordinate_list_url": WIKI_DISTRICTS_URL,
            "coordinate_api_url": WIKI_API_URL,
            "total_districts": len(records),
            "with_coordinates": len(records) - len(missing),
            "missing_coordinates": len(missing),
            "manual_overrides": len(MANUAL_COORDINATE_OVERRIDES),
            "lookup_key_format": "normalized provincia|canton|distrito",
        },
        "districts": records,
        "lookup": lookup,
        "missing": missing,
    }


def main() -> None:
    output = Path("data/reference/costa_rica_district_coordinates_cache.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    cache = build_cache()
    output.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {output}")
    print(
        "districts={total_districts} with_coordinates={with_coordinates} missing={missing_coordinates}".format(
            **cache["metadata"]
        )
    )
    if cache["missing"]:
        print("Missing examples:")
        for item in cache["missing"][:10]:
            print(f"  {item['codigo']}: {item['provincia']} / {item['canton']} / {item['distrito']} - {item['reason']}")


if __name__ == "__main__":
    main()
