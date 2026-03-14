"""
Data Processing Service Functions

Name: Arowosegbe Victor\n
Email: Iyanuvicky@gmail.com\n
GitHub: https://github.com/Iyanuvicky22/projects
"""

import os
import json
from datetime import datetime

from processor import load_data as ld
from processor import clean as cl
from processor import aggregate as ag
from processor.utils import utils
from processor.utils.logger import logger


RAW_PANDAS_DF = None
RAW_POLARS_DF = None

CURRENT_DATE = datetime.now().strftime("%d-%m-%Y")
DATA_DIR = "data"


def ensure_data_loaded():
    global RAW_PANDAS_DF, RAW_POLARS_DF

    if RAW_PANDAS_DF is None:
        RAW_PANDAS_DF = ld.read_pandas()

    if RAW_POLARS_DF is None:
        RAW_POLARS_DF = ld.read_polars()


def preprocess_pandas():

    ensure_data_loaded()

    df = cl.pd_na_handler(RAW_PANDAS_DF)
    df = cl.handle_outlier_pandas(df, col="Quantity", method="cap")
    df = cl.handle_outlier_pandas(df, col="Price", method="cap")
    df = cl.transform_df(df)

    return df


def preprocess_polars():

    ensure_data_loaded()

    df = cl.pl_na_handler(RAW_POLARS_DF)
    df = cl.handle_outlier_polars(df, column="Quantity", method="drop")
    df = cl.handle_outlier_polars(df, column="Price", method="drop")
    df = cl.transform_df(df)

    return df


def visualize_data():

    pandas_df = preprocess_pandas()
    polars_df = preprocess_polars()

    utils.viz_data(pandas_df)
    utils.viz_data(polars_df)

    return {"message": "Visualization generated", "success": True}


def process_data():

    pandas_df = preprocess_pandas()
    polars_df = preprocess_polars()

    pandas_agg = ag.aggregate_pandas(pandas_df)
    polars_agg = ag.aggregate_polars(polars_df)

    return {
        "message": "Processing successful",
        "data": {
            "pandas": json.loads(pandas_agg.to_json()),
            "polars": json.loads(polars_agg.write_json()),
        },
    }


def benchmark_pandas_pipeline():
    timings = {}

    pd_df, load_time = ld.measure_time(ld.read_pandas)
    timings["load_time_seconds"] = load_time
    timings["memory_usage_mb"] = round(
        pd_df.memory_usage(deep=True).sum() / (1024**2), 4
    )

    df = cl.pd_na_handler(pd_df)
    df = cl.handle_outlier_pandas(df, col="Quantity", method="cap")
    df = cl.handle_outlier_pandas(df, col="Price", method="cap")
    df = cl.transform_df(df)

    _, viz_time = ld.measure_time(utils.viz_data, df)
    timings["visualization_time_seconds"] = viz_time

    _, processing_time = ld.measure_time(ag.aggregate_pandas, df)
    timings["processing_time_seconds"] = processing_time

    def export_pandas():
        export_df = df.copy()
        export_df["Description"] = export_df["Description"].astype(str)
        file_path = f"data/pandas_data_{CURRENT_DATE}.parquet"
        export_df.to_parquet(file_path, engine="pyarrow", compression="snappy")
        return file_path

    _, export_time = ld.measure_time(export_pandas)
    timings["export_time_seconds"] = export_time

    timings["total_time_seconds"] = round(
        timings["load_time_seconds"]
        + timings["processing_time_seconds"]
        + timings["visualization_time_seconds"]
        + timings["export_time_seconds"],
        6,
    )
    return timings


def benchmark_polars_pipeline():
    timings = {}

    pl_df, load_time = ld.measure_time(ld.read_polars)
    timings["load_time_seconds"] = load_time
    timings["memory_usage_mb"] = round(pl_df.estimated_size() / (1024**2), 4)

    df = cl.pl_na_handler(pl_df)
    df = cl.handle_outlier_polars(df, column="Quantity", method="drop")
    df = cl.handle_outlier_polars(df, column="Price", method="drop")
    df = cl.transform_df(df)

    _, viz_time = ld.measure_time(utils.viz_data, df)
    timings["visualization_time_seconds"] = viz_time

    _, processing_time = ld.measure_time(ag.aggregate_polars, df)
    timings["processing_time_seconds"] = processing_time

    def export_polars():
        file_path = f"data/polars_data_{CURRENT_DATE}.json"
        df.write_json(file_path)
        return file_path

    _, export_time = ld.measure_time(export_polars)
    timings["export_time_seconds"] = export_time

    timings["total_time_seconds"] = round(
        timings["load_time_seconds"]
        + timings["processing_time_seconds"]
        + timings["visualization_time_seconds"]
        + timings["export_time_seconds"],
        6,
    )
    return timings


def percent_improvement(pandas_time, polars_time):
    if pandas_time == 0:
        return 0
    return round(((pandas_time - polars_time) / pandas_time) * 100, 2)


def speed_ratio(pandas_time, polars_time):
    if polars_time == 0:
        return None
    return round(pandas_time / polars_time, 2)


def compare_time():
    """
    Compare Pandas vs Polars across:
    - data loading
    - visualization
    - processing
    - exporting
    """
    try:
        pandas_timings = benchmark_pandas_pipeline()
        polars_timings = benchmark_polars_pipeline()

        comparison = {
            "load_speedup_percent": percent_improvement(
                pandas_timings["load_time_seconds"],
                polars_timings["load_time_seconds"]
            ),
            "processing_speedup_percent": percent_improvement(
                pandas_timings["processing_time_seconds"],
                polars_timings["processing_time_seconds"],
            ),
            "visualization_speedup_percent": percent_improvement(
                pandas_timings["visualization_time_seconds"],
                polars_timings["visualization_time_seconds"],
            ),
            "export_speedup_percent": percent_improvement(
                pandas_timings["export_time_seconds"],
                polars_timings["export_time_seconds"],
            ),
            "load_speed_ratio": speed_ratio(
                pandas_timings["load_time_seconds"],
                polars_timings["load_time_seconds"]
            ),
            "processing_speed_ratio": speed_ratio(
                pandas_timings["processing_time_seconds"],
                polars_timings["processing_time_seconds"],
            ),
            "visualization_speed_ratio": speed_ratio(
                pandas_timings["visualization_time_seconds"],
                polars_timings["visualization_time_seconds"],
            ),
            "export_speed_ratio": speed_ratio(
                pandas_timings["export_time_seconds"],
                polars_timings["export_time_seconds"],
            ),
        }

        return {
            "message": "Time Comparison Results",
            "success": True,
            "data": {
                "pandas": pandas_timings,
                "polars": polars_timings,
                "performance_comparison": comparison,
            },
        }

    except Exception as e:
        logger.error(f"Time comparison failed: {e}")

        return {"message": "Time Comparison Results",
                "success": False, 
                "error": str(e)}


def export_polars_json():

    os.makedirs(DATA_DIR, exist_ok=True)
    df = preprocess_polars()
    path = f"{DATA_DIR}/polars_data_{CURRENT_DATE}.json"
    df.write_json(path)

    return path


def export_pandas_parquet():

    os.makedirs(DATA_DIR, exist_ok=True)
    df = preprocess_pandas()
    df["Description"] = df["Description"].astype(str)
    path = f"{DATA_DIR}/pandas_data_{CURRENT_DATE}.parquet"
    df.to_parquet(path, engine="pyarrow", compression="snappy")

    return path
