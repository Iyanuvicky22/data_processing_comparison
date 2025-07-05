"""
Loading of extracted data using Pandas and Polars.

Name: Arowosegbe Victor\n
Email: Iyanuvicky@gmail.com\n
GitHub: https://github.com/Iyanuvicky22/data_processing_comparison
"""

import time
import pandas as pd
import polars as pl
from processor.utils.logger import logger


def read_pandas() -> pd.DataFrame:
    """
    This function reads the year 2009-2010
    xlxs file using pandas.
    """
    try:
        df_pd = pd.read_excel("data/online_retail_II.xlsx", sheet_name=0)
        return df_pd
    except Exception as e:
        logger.error("Error encountered:", e)


def read_polars() -> pl.DataFrame:
    """
    This function reads the year 2010-2011
    xlxs file using polars.
    """
    try:
        df_pl = pl.read_excel("data/online_retail_II.xlsx", sheet_id=1)
        return df_pl
    except Exception as e:
        logger.error("Error encountered:", e)


def compare_time(pd_func, pl_func, action="Loading Time") -> list:
    """
    This function compares loading
    times of the above two functions.
    """
    try:
        start = time.time()
        pd_func()
        end = time.time()

        start_2 = time.time()
        pl_func()
        end_2 = time.time()

        res = [
            {f"Pandas {action}": round((end - start), 2)},
            {f"Polars {action}": round((end_2 - start_2), 2)},
        ]
    except Exception as e:
        logger.error("Error encountered:", e)
    return res
