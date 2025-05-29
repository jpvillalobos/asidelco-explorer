#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from openai import OpenAI
from opensearchpy import OpenSearch
import warnings
import urllib3

# Disable certificate warnings (for local testing only)
warnings.filterwarnings("ignore", category=UserWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuración de OpenSearch
client_os = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "OpenSearch123!"),
    use_ssl=True,
    verify_certs=False
)

index_name = "cfia-projects"

# Consulta en español
query = "Proyecto de vivienda de interés social en Limón para familia de bajos ingresos"

# Obtener embedding de OpenAI
client_ai = OpenAI()
embedding_response = client_ai.embeddings.create(
    input=query,
    model="text-embedding-3-small"
)
query_vector = embedding_response.data[0].embedding

# Ejecutar búsqueda KNN en OpenSearch
response = client_os.search(
    index=index_name,
    body={
        "size": 5,
        "query": {
            "knn": {
                "embeddings.text-embedding-3-small": {
                    "vector": query_vector,
                    "k": 5
                }
            }
        },
        "_source": ["project_id.value", "resumen.value"]
    }
)

# Mostrar resultados
print("\nTop 5 proyectos más similares:")
for hit in response["hits"]["hits"]:
    proj_id = hit["_source"].get("project_id", {}).get("value", hit["_id"])
    resumen = hit["_source"].get("resumen", {}).get("value", "Sin resumen")
    score = hit["_score"]
    print(f"\n- ID: {proj_id}\n  Score: {score:.4f}\n  Resumen: {resumen}")
