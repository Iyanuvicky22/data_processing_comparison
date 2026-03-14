from fastapi import APIRouter
from fastapi.responses import FileResponse

from app.services import processing_service as service

router = APIRouter()


@router.get("/home")
def home():
    return {"message": "Welcome to the Pandas vs Polars API"}


@router.get("/visualization")
async def visualization():
    return service.visualize_data()


@router.get("/data-processing")
async def processing():
    return service.process_data()


@router.get("/time-comparison")
async def time_compare():
    return service.compare_time()


@router.get("/download-json")
def download_json():
    path = service.export_polars_json()
    return FileResponse(path, media_type="application/json", filename="polars_data.json")


@router.get("/download-parquet")
def download_parquet():
    path = service.export_pandas_parquet()
    return FileResponse(path, media_type="application/octet-stream", filename="pandas_data.parquet")