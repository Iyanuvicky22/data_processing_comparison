from fastapi import FastAPI
from app.routers.processing_router import router as processing_router

app = FastAPI(
    title="Data Processing Comparison API",
    description="Compare Pandas vs Polars performance in a FastAPI service",
    version="0.1.0",
)

app.include_router(processing_router)
