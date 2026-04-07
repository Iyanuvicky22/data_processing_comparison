"""
This function perform aggregation
of the dataset using pandas and polars.

Name: Arowosegbe Victor\n
Email: Iyanuvicky@gmail.com\n
GitHub: https://github.com/Iyanuvicky22/data_processing_comparison
"""

import pandas as pd
import polars as pl
from processor.utils.logger import logger


def aggregate_pandas(df: pd.DataFrame):
    """
    Aggregation function using pandas

    Args:
        df (pd.DataFrame): _description_
    """
    try:
        return round(
            df.groupby(["NameOfDay"])["Price"].agg(
                ["sum", "mean", "median", "count"]), 2).round(2)
    except Exception as e:
        logger.error("Error while aggregating Pandas DataFrame: %s", e)
        return df


def aggregate_polars(df: pl.DataFrame):
    """
    Aggregation function using pandas

    Args:
        df (pl.DataFrame): _description_
    """
    try:
        return df.group_by("NameOfDay").agg(
            [
                pl.col("Price").sum().round(2).alias("sum"),
                pl.col("Price").mean().round(2).alias("mean"),
                pl.col("Price").median().round(2).alias("median"),
                pl.col("Price").count().alias("count"),
            ]
        )
    except Exception as e:
        logger.error("Error while aggregating Polars DataFrame: %s", e)
    return df
