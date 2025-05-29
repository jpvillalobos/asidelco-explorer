from fastapi import FastAPI, Request
from pydantic import BaseModel

app = FastAPI()

class Question(BaseModel):
    question: str

@app.post("/ask")
async def ask(q: Question):
    response = final_chain.invoke(q.question)
    return {"response": response}
