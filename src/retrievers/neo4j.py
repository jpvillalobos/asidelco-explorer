#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from langchain_openai import ChatOpenAI
from langchain_neo4j import Neo4jGraph
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda, RunnableSequence

# ─────────── Conexión Neo4j (sin APOC) ─────────── #
graph = Neo4jGraph(
    url="bolt://localhost:7687",
    username="neo4j",
    password="your_password",
    refresh_schema=False      # evita apoc.meta.data()
)

# ─────────── Extraer esquema (labels y relaciones) ─────────── #
schema_row = graph.query("""
CALL db.labels() YIELD label
WITH collect(label) AS labels
CALL db.relationshipTypes() YIELD relationshipType
WITH labels, collect(relationshipType) AS rels
RETURN labels, rels
""")[0]

labels = schema_row["labels"]          # p. ej. ['Project','Professional',...]
rels   = schema_row["rels"]            # p. ej. ['DESIGNED_BY','LOCATED_IN',...]

schema_text = (
    "### Esquema Neo4j\n"
    + "\n".join(f"(:{l})" for l in labels)
    + "\n"
    + "\n".join(f"-[:{r}]-" for r in rels)
)

# ─────────── LLM OpenAI ─────────── #
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

# Prompt: incluye esquema y exige SOLO Cypher
prompt = PromptTemplate(
    input_variables=["schema", "question"],
    template=(
        "Eres un experto en Cypher.\n"
        "Utiliza exclusivamente las etiquetas y relaciones listadas en el esquema.\n"
        "{schema}\n\n"
        "Devuelve SOLO la consulta Cypher que responde la pregunta, sin explicación ni backticks.\n\n"
        "Pregunta: {question}\nCypher:"
    ),
)

# ─────────── Limpiador de Cypher ─────────── #
def extract_cypher(raw: str) -> str:
    # bloque ```cypher ...```
    code_block = re.search(r"```cypher\\s*(.+?)```", raw, re.S | re.I)
    if code_block:
        return code_block.group(1).strip()
    # primera línea que empiece con palabra clave Cypher
    match = re.search(r"(?i)(MATCH|CALL|CREATE|MERGE|UNWIND|WITH|RETURN).*", raw, re.S)
    if match:
        return match.group(0).strip()
    return raw.strip()

# ─────────── Pipeline LangChain ─────────── #
chain = RunnableSequence(
    prompt,                       # llena prompt con schema+question
    llm,                          # genera respuesta
    StrOutputParser(),            # to string
    RunnableLambda(extract_cypher)  # limpia salida
)

def query_graph(question: str):
    cypher = chain.invoke({"schema": schema_text, "question": question})
    print("Cypher generado:\n", cypher)
    return graph.query(cypher)

# ─────────── Ejemplo ─────────── #
if __name__ == "__main__":
    pregunta = (
        "¿Qué proyectos ubicados en la provincia de Puntarenas fueron diseñados por IC-22826"
    )
    resultados = query_graph(pregunta)
    for row in resultados:
        print(row)
