import json
from sentence_transformers import SentenceTransformer
from opensearchpy import OpenSearch

# Load and flatten JSON
with open('data.json') as f:
    data = json.load(f)

# Convert to readable text
def record_to_text(record):
    return f"Employee {record['employee_id']}: {record['name']} works in {record['department']}. Skills: {', '.join(record['skills'])}."

chunks = [record_to_text(rec) for rec in data]

# Embed the text
model = SentenceTransformer('all-MiniLM-L6-v2')  # Or use OpenAI embeddings
vectors = model.encode(chunks)

# Push to OpenSearch
os_client = OpenSearch(hosts=[{'host': 'localhost', 'port': 9200}])

for i, chunk in enumerate(chunks):
    doc = {
        'text': chunk,
        'embedding': vectors[i].tolist()
    }
    os_client.index(index="employee-knowledge", body=doc)
