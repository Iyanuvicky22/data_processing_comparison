"""
Loading of extracted data using Pandas and Polars.

Name: Arowosegbe Victor
Email: Iyanuvicky@gmail.com
GitHub: https://github.com/Iyanuvicky22/data_processing_comparison
"""

import time
import pandas as pd
import polars as pl
from processor.utils.logger import logger


def read_pandas() -> pd.DataFrame:
    """
    Read the 2009-2010 Excel sheet using pandas.
    """
    try:
        return pd.read_excel("data/online_retail_II.xlsx", sheet_name=0)
    except Exception as e:
        logger.error(f"Error reading pandas data: {e}")
        raise


def read_polars() -> pl.DataFrame:
    """
    Read the 2010-2011 Excel sheet using polars.
    """
    try:
        return pl.read_excel("data/online_retail_II.xlsx", sheet_id=1)
    except Exception as e:
        logger.error(f"Error reading polars data: {e}")
        raise


def measure_time(func, *args, **kwargs):
    """
    Measure execution time of a function and return:
    (result, elapsed_time_seconds)
    """
    try:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        return result, round(end - start, 6)
    except Exception as e:
        logger.error(f"Error timing function {func.__name__}: {e}")
        raise
