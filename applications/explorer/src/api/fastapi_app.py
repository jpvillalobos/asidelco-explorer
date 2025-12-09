from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .pipeline_routes import router as pipeline_router
from .websocket_routes import router as ws_router

app = FastAPI(title="Asidelco Explorer API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(pipeline_router)
app.include_router(ws_router)

@app.get("/")
async def root():
    return {"message": "Asidelco Explorer API", "version": "1.0.0"}

@app.get("/health")
async def health():
    return {"status": "healthy"}
