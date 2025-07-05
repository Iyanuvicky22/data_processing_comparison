import pandas as pd
import polars as pl
import plotly.express as px
from processor.utils.logger import logger


# Setting Display Options
pd.set_option("display.max_columns", None)
pl.Config.set_tbl_rows(10)
pl.Config.set_tbl_cols(-1)

COLS = [
    "Quantity",
    "Price",
]


def viz_data(df: pd.DataFrame | pl.DataFrame, columns=None):
    """
    Function to visualize pandas & polars dataframes
    and visually check for outliers.
    """
    try:
        if columns is None:
            columns = COLS
            for col in columns:
                fig = px.box(
                    df,
                    col,
                    points="outliers",
                    width=500,
                    height=400,
                    title=f"{col} Column Outlier Detection Chart!",
                )
                fig.show()
        else:
            for col in columns:
                fig = px.box(
                    df,
                    col,
                    points="outliers",
                    width=500,
                    height=400,
                    title=f"{col} Column Outlier Detection Chart!",
                )
                fig.show()
    except AttributeError as e:
        logger.error(
            f"Data loaded is not a polars or pandas dataframe.\
        It is {e}"
        )


def check_outliers_info_pandas(df: pd.DataFrame, col: str):
    """
    Finds Outliers in a column

    Args:
        df (Dataframe): Pandas Dataframe
    """
    try:
        columns = df.columns.tolist()
        if col in columns:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)

            iqr = q3 - q1

            outliers = df[col][
                ((df[col] < (q1 - 1.5 * iqr)) | (df[col] > (q3 + 1.5 * iqr)))
            ]

            num_of_outliers = len(outliers)
            max_outlier = outliers.max()
            min_outlier = outliers.min()

            return {
                "Column Name": col,
                "Total Outliers": num_of_outliers,
                "Max Outlier Value": max_outlier,
                "Min Outlier Value": min_outlier,
            }
    except NameError:
        logger.error("Column not in dataframe columns.")
    except TypeError:
        logger.error("Column not integer or float type.")


def check_outliers_info_polars(df: pl.DataFrame, col: str):
    """
    Get Outliers Info from polars dataframe
    Args:
        df (pl.DataFrame): Polars Dataframe
        col (str): Column name
    """
    try:
        columns = df.columns
        if col in columns:
            q1 = df.select(pl.col(col).quantile(0.25)).to_numpy()[0][0]
            q3 = df.select(pl.col(col).quantile(0.75)).to_numpy()[0][0]

            iqr = q3 - q1

            outliers = df.filter(
                (pl.col(col) < (q1 - 1.5 * iqr)) | (pl.col(col) > (q3 + 1.5 * iqr))
            )

            num_of_outliers = len(outliers)
            max_outlier = outliers.max()
            min_outlier = outliers.min()

            return {
                "Column Name": col,
                "Total Outliers": num_of_outliers,
                "Max Outlier Value": max_outlier,
                "Min Outlier Value": min_outlier,
            }
    except NameError:
        print("Column not in dataframe columns.")
    except TypeError:
        print("Column not integer or float type.")
