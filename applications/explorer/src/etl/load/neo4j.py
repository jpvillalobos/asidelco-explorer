#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
from neo4j import GraphDatabase
from datetime import datetime

# Configuration
ENHANCED_JSON_DIR = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/projects/enhanced"
ERROR_LOG_PATH = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/logs/neo4j_load_errors.log"

class GraphLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def load_project(self, doc):
        with self.driver.session() as session:
            session.write_transaction(self._create_project_graph, doc)

    @staticmethod
    def _create_project_graph(tx, doc):
        def val(field):
            return doc.get(field, {}).get("value")

        tx.run("""
        MERGE (proj:Project {id: $project_id})
          ON CREATE SET proj.name = $name,
                        proj.estado = $estado,
                        proj.descripcion = $descripcion,
                        proj.clasificacion = $clasificacion,
                        proj.tasado = $tasado,
                        proj.fecha = $fecha,
                        proj.resumen = $resumen

        MERGE (prof:Professional {carnet: $prof_carnet})
          ON CREATE SET prof.name = $prof_name,
                        prof.email = $prof_email,
                        prof.telefono = $prof_tel,
                        prof.direccion = $prof_direccion,
                        prof.cedula = $prof_cedula,
                        prof.colegio = $prof_colegio

        MERGE (loc:Location {provincia: $provincia, canton: $canton, distrito: $distrito})

        MERGE (proj)-[:DESIGNED_BY]->(prof)
        MERGE (proj)-[:LOCATED_IN]->(loc)

        FOREACH (_ IN CASE WHEN $company_name IS NOT NULL THEN [1] ELSE [] END |
          MERGE (comp:Company {name: $company_name})
          MERGE (proj)-[:BUILT_BY]->(comp)
        )
        """, {
            "project_id": val("project_id"),
            "name": val("descripcion_proyecto"),
            "estado": val("estado_proyecto"),
            "descripcion": val("detalle_proyecto"),
            "clasificacion": val("clasificacion"),
            "tasado": val("valor_tasado"),
            "fecha": val("fecha_proyecto"),
            "resumen": val("resumen"),

            "prof_carnet": val("carnet_profesional"),
            "prof_name": val("nombre_profesional"),
            "prof_email": val("email_profesional_laboral"),
            "prof_tel": val("telefono_profesional_movil"),
            "prof_direccion": val("direccion_profesional"),
            "prof_cedula": val("cedula_profesional"),
            "prof_colegio": val("colegio_profesional"),

            "provincia": val("provincia"),
            "canton": val("canton"),
            "distrito": val("distrito"),

            "company_name": val("empresa_responsable")
        })

def log_error(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(ERROR_LOG_PATH, "a", encoding="utf-8") as log:
        log.write(f"[{timestamp}] {message}\n")

def load_all_projects(directory, loader):
    files = sorted(f for f in os.listdir(directory) if f.endswith(".json"))
    total = len(files)
    count = 0
    start_time = time.time()

    print(f"Starting import of {total} project files...")

    for filename in files:
        count += 1
        filepath = os.path.join(directory, filename)

        try:
            with open(filepath, "r", encoding="utf-8") as file:
                project_data = json.load(file)
                loader.load_project(project_data)

                elapsed = time.time() - start_time
                hrs, rem = divmod(int(elapsed), 3600)
                mins, secs = divmod(rem, 60)
                print(f"[{count}/{total}] Loaded: {filename} - Elapsed: {hrs:02}:{mins:02}:{secs:02}")
        except Exception as e:
            log_error(f"Failed to load {filename}: {e}")
            print(f"[{count}/{total}] Error loading {filename} (logged)")

    print("Import completed.")

if __name__ == "__main__":
    loader = GraphLoader("bolt://localhost:7687", "neo4j", "your_password")  # ← change password here
    load_all_projects(ENHANCED_JSON_DIR, loader)
    loader.close()
