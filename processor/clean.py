"""
This script performs data cleaning
on the loaded datasets

Name: Arowosegbe Victor\n
Email: Iyanuvicky@gmail.com\n
GitHub: https://github.com/Iyanuvicky22/projects
"""

import pandas as pd
import polars as pl
import numpy as np
from processor.utils.logger import logger


def pd_na_handler(df: pd.DataFrame):
    """
    Description column has 2928 missing values.
    - That is 1.6% of total data.
    Decision :- It will be dropped.
    Customer ID has 107927 missing values.
    - That is 20.5% of total data.
    Decision :- It will be filled. 'ffill'
    """
    try:
        df = df.dropna(subset=["Description"])
        df.loc[:, "Customer ID"] = df["Customer ID"].ffill()
        return df
    except Exception as e:
        logger.error("Error Encountered", e)


def pl_na_handler(df: pl.DataFrame):
    """
    Invoice column has 10209 missing values.
    - That is 1.9% of total data.
    Decision :- It will be dropped.
    Description column has 2928 missing values.
    - That is 1.6% of total data.
    Decision :- It will be dropped.
    Customer ID has 107927 missing values.
    - That is 20.5% of total data.
    Decision :- It will be filled."""
    try:
        df = df.drop_nulls(subset=["Invoice", "Description"])
        df = df.fill_null(strategy="forward")
        return df
    except Exception as e:
        logger.error("Error Encountered", e)


def handle_outlier_pandas(df: pd.DataFrame, col: str, method="cap"):
    """
    This function handles the
    identified outliers in the pandas dataframe
    Args:
        df (pd.DataFrame): Pandas Dataframe
        col (str): Column with outliers
        method (str, optional): Method to use in handling outliers.
        Defaults to 'cap'.
    """
    q1 = df[col].quantile(0.25)
    q3 = df[col].quantile(0.75)

    iqr = q3 - q1

    try:
        if method == "drop":
            df[col] = df[col][
                ~((df[col] < (q1 - 1.5 * iqr)) | (df[col] > (q3 + 1.5 * iqr)))
            ]
            df = df.dropna()
            return df
        if method == "cap":
            upper_limit = df[col].mean() + 3 * df[col].std()
            lower_limit = df[col].mean() - 3 * df[col].std()
            df[col] = np.where(
                df[col] > upper_limit,
                upper_limit,
                np.where(df[col] < lower_limit, lower_limit, df[col]),
            )
            return df
        if method == "mean":
            lower_limit = df[col][~(df[col] < (q1 - 1.5 * iqr))].max()
            upper_limit = df[col][~(df[col] > (q3 + 1.5 * iqr))].min()
            df[col] = np.where(
                df[col] > upper_limit,
                df[col].mean(),
                np.where(df[col] < lower_limit, df[col].mean(), df[col]),
            )
            return df
    except NameError:
        logger.error("Column not in dataframe columns.")
    except AttributeError:
        logger.error("Dataframe not of pandas type")


def handle_outlier_polars(df: pl.DataFrame, col: str, method="cap"):
    """
    This function handles the
    identified outliers in the polars dataframe
    Args:
        df (pd.DataFrame): Pandas Dataframe
        col (str): Column with outliers
        method (str, optional): Method to use in handling outliers.
        Defaults to 'cap'.
    """
    df = df.to_pandas()
    q1 = df[col].quantile(0.25)
    q3 = df[col].quantile(0.75)

    iqr = q3 - q1

    try:
        if method == "drop":
            df[col] = df[col][
                ~((df[col] < (q1 - 1.5 * iqr)) | (df[col] > (q3 + 1.5 * iqr)))
            ]
            df = df.dropna()
            df = pl.from_pandas(df)
            return df
        if method == "cap":
            upper_limit = df[col].mean() + 3 * df[col].std()
            lower_limit = df[col].mean() - 3 * df[col].std()
            df[col] = df[col].astype(float)
            df[col] = np.where(
                df[col] > upper_limit,
                upper_limit,
                np.where(df[col] < lower_limit, lower_limit, df[col]),
            )
            df[col] = df[col].astype(int)
            df = pl.from_pandas(df)
            return df
        if method == "mean":
            lower_limit = df[col][~(df[col] < (q1 - 1.5 * iqr))].max()
            upper_limit = df[col][~(df[col] > (q3 + 1.5 * iqr))].min()
            df[col] = df[col].astype(float)
            df[col] = np.where(
                df[col] > upper_limit,
                df[col].mean(),
                np.where(df[col] < lower_limit, df[col].mean(), df[col]),
            )
            df[col] = df[col].astype(int)
            df = pl.from_pandas(df)
            return df
    except NameError:
        logger.error("Column not in dataframe columns.")
    except AttributeError:
        logger.error("Dataframe not of polars type")
    except Exception as e:
        logger.error("Error Encountered", e)


def transform_df(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame | pl.DataFrame:
    """
    Data Transformation and new columns creation.

    Args:
        df (Dataframe): Pandas or Polars Dataframe.
    """
    try:
        if isinstance(df, pl.DataFrame):
            columns = df.columns
            col = "InvoiceDate"
            if col in columns:
                df = df.rename({"InvoiceDate": "InvoiceDateTime"})
                df = df.with_columns(
                    pl.col("InvoiceDateTime").dt.date().alias("InvoiceDate")
                )
                df = df.with_columns(
                    pl.col("InvoiceDateTime").dt.time().alias("InvoiceTime")
                )
                df = df.with_columns(
                    pl.col("InvoiceDateTime").dt.strftime("%A").alias("NameOfDay")
                )
            return df
        if isinstance(df, pd.DataFrame):
            columns = df.columns.tolist()
            col = ["InvoiceDate", "Invoice", "StockCode"]

            for col in columns:
                if col == "InvoiceDate":
                    df = df.rename(columns={"InvoiceDate": "InvoiceDateTime"})
                    df["InvoiceDateTime"] = pd.to_datetime(df["InvoiceDateTime"])
                    df["InvoiceDate"] = df["InvoiceDateTime"].dt.date
                    df["InvoiceTime"] = df["InvoiceDateTime"].dt.time
                    df["NameOfDay"] = df["InvoiceDateTime"].dt.day_name()
                if col == "Invoice":
                    df.loc[:, "Invoice"] = df.loc[:, "Invoice"].astype(str)
                if col == "StockCode":
                    df.loc[:, "StockCode"] = df.loc[:, "StockCode"].astype(str)
            return df
    except AttributeError:
        logger.error(
            "\t\tData is not a dataframe.\n\
            Kindly input a dataframe of pandas or polars type."
        )
        return None
    except Exception as e:
        logger.error("Error Encountered", e)
