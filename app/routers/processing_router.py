"""
Data Processing Routers

Name: Arowosegbe Victor\n
Email: Iyanuvicky@gmail.com\n
GitHub: https://github.com/Iyanuvicky22/projects
"""
from fastapi import APIRouter
from fastapi.responses import FileResponse

from app.services import processing_service as service
from processor.utils.logger import logger

router = APIRouter()


@router.get("/home")
def home():
    return {"message": "Welcome to the Pandas vs Polars API"}


@router.get("/visualization")
async def visualization():
    result = service.visualize_data()
    logger("Data Visualization successful!")
    return result


@router.get("/data-processing")
async def processing():
    result = service.process_data()
    logger.info("Data Processing Successful!")
    return result


@router.get("/time-comparison")
async def time_compare():
    result = service.compare_time()
    logger.info("Benchmark Time Comparison successful!")
    return result


@router.get("/polars_export_json")
def download_json():
    path = service.export_polars_json()
    result = FileResponse(
        path, media_type="application/json", filename="polars_data.json"
    )
    logger.info("Polars Data Export as Json file successful!")
    return result


@router.get("/pandas_export_parquet")
def download_parquet():
    path = service.export_pandas_parquet()
    result = FileResponse(
        path, media_type="application/octet-stream",
        filename="pandas_data.parquet"
    )
    logger.info("Pandas Data Export as Parquet Successful!")
    return result
