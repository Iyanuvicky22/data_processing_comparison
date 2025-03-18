"""
This script performs data cleaning
on the loaded datasets
"""
import pandas as pd
import polars as pl
import numpy as np
import plotly.express as px

# Setting Display Options
pd.set_option('display.max_columns', None)
pl.Config.set_tbl_rows(10)
pl.Config.set_tbl_cols(-1)

COLS = ['Quantity',
        'Price',
        ]


def viz_data(df, columns=None):
    """
    Function to visualize pandas & polars dataframes
    and visually check for outliers.
    """
    try:
        if isinstance(df, pd.DataFrame):
            if columns is None:
                columns = COLS
                for col in columns:
                    fig = px.box(df, col)
                    fig.show()
            else:
                for col in columns:
                    fig = px.box(df, col)
                    fig.show()
        elif isinstance(df, pl.DataFrame):
            if columns is None:
                columns = COLS
                for col in columns:
                    fig = px.box(df, col)
                    fig.show()
            else:
                for col in columns:
                    fig = px.box(df, col)
                    fig.show()
    except AttributeError as e:
        print(f'Data loaded is not a polars or pandas dataframe.\
        It is {e}')


def pd_na_handler(df: pd.DataFrame):
    """
    Description column has 2928 missing values.
    - That is 1.6% of total data.
    Decision :- It will be dropped.
    Customer ID has 107927 missing values.
    - That is 20.5% of total data.
    Decision :- It will be filled. 'ffill'
    """
    # Handling missing values based on docstring info.
    df = df.dropna(subset=['Description'])
    df.loc[:, 'Customer ID'] = df['Customer ID']\
        .ffill()
    return df


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

    # Handling missing data with polars based on docstring info.
    df = df.drop_nulls(subset=['Invoice',
                               'Description'])
    df = df.fill_null(strategy='forward')

    return df


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

            iqr = q3-q1

            outliers = df[col][((df[col] < (q1 - 1.5 * iqr)) |
                                (df[col] > (q3 + 1.5 * iqr)))]

            num_of_outliers = str(len(outliers))
            max_outlier = str(outliers.max())
            min_outlier = str(outliers.min())

            return f'\t\tColumn Name: {df.name}\n\
                    Total Outliers: {num_of_outliers}\n\
                    Max Outlier Value: {max_outlier}\n\
                    Min Outlier Value: {min_outlier}\n'
    except NameError:
        print('Column not in dataframe columns.')
    except TypeError:
        print('Column not integer or float type.')


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
            print(q1, q3)

            iqr = q3 - q1

            outliers = df.filter((pl.col(col) < (q1 - 1.5 * iqr)) |
                                 (pl.col(col) > (q3 + 1.5 * iqr)))

            num_of_outliers = str(len(outliers))
            max_outlier = str(outliers.max())
            min_outlier = str(outliers.min())

            return f'\t\tColumn Name: {col}\n\
                    Total Outliers: {num_of_outliers}\n\
                    Max Outlier Value: {max_outlier}\n\
                    Min Outlier Value: {min_outlier}\n'
    except NameError:
        print('Column not in dataframe columns.')
    except TypeError:
        print('Column not integer or float type.')


def handle_outlier_pandas(df: pd.DataFrame, col: str, method='cap'):
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

    iqr = q3-q1

    try:
        if method == 'drop':
            df[col] = df[col][~((df[col] < (q1 - 1.5 * iqr)) |
                                (df[col] > (q3 + 1.5 * iqr)))]
            df = df.dropna()
            return df
        if method == 'cap':
            upper_limit = df[col].mean() + 3 * df[col].std()
            lower_limit = df[col].mean() - 3 * df[col].std()
            print(lower_limit, upper_limit)
            df[col] = np.where(df[col] > upper_limit, upper_limit,
                               np.where(df[col] < lower_limit, lower_limit,
                               df[col]))
            return df
        if method == 'mean':
            lower_limit = df[col][~(df[col] < (q1 - 1.5 * iqr))].max()
            upper_limit = df[col][~(df[col] > (q3 + 1.5 * iqr))].min()
            print(upper_limit, lower_limit)

            df[col] = np.where(df[col] > upper_limit, df[col].mean(),
                               np.where(df[col] < lower_limit, df[col].mean(),
                               df[col]))
            return df
    except NameError:
        print('Column not in dataframe columns.')
    except AttributeError:
        print('Dataframe not of pandas type')


def handle_outlier_polars(df: pl.DataFrame, col: str, method='cap'):
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
        if method == 'drop':
            df[col] = df[col][~((df[col] < (q1 - 1.5 * iqr)) |
                                (df[col] > (q3 + 1.5 * iqr)))]
            df = df.dropna()
            print(df.describe()[[col]])
            df = pl.from_pandas(df)
            return df
        if method == 'cap':
            upper_limit = df[col].mean() + 3 * df[col].std()
            lower_limit = df[col].mean() - 3 * df[col].std()
            print(lower_limit, upper_limit)
            df[col] = df[col].astype(float)
            df[col] = np.where(df[col] > upper_limit, upper_limit,
                               np.where(df[col] < lower_limit, lower_limit,
                               df[col]))
            df[col] = df[col].astype(int)
            print(df.describe()[[col]])
            df = pl.from_pandas(df)
            return df
        if method == 'mean':
            lower_limit = df[col][~(df[col] < (q1 - 1.5 * iqr))].max()
            upper_limit = df[col][~(df[col] > (q3 + 1.5 * iqr))].min()
            print(upper_limit, lower_limit)
            df[col] = df[col].astype(float)
            df[col] = np.where(df[col] > upper_limit, df[col].mean(),
                               np.where(df[col] < lower_limit, df[col].mean(),
                               df[col]))
            df[col] = df[col].astype(int)
            print(df.describe()[[col]])
            df = pl.from_pandas(df)
            return df
    except NameError:
        print('Column not in dataframe columns.')
    except AttributeError:
        print('Dataframe not of polars type')


def transform_df(df):
    """
    Data Transformation and new columns creation.

    Args:
        df (Dataframe): Pandas and Polars Dataframe.
    """
    try:
        if isinstance(df, pl.DataFrame):
            columns = df.columns
            col = 'InvoiceDate'
            if col in columns:
                df = df.rename({'InvoiceDate': 'InvoiceDateTime'})
                df = df.with_columns(pl.col('InvoiceDateTime').dt.date().alias
                                     ('InvoiceDate'))
                df = df.with_columns(pl.col('InvoiceDateTime').dt.time().alias
                                     ('InvoiceTime'))
                df = df.with_columns(pl.col('InvoiceDateTime').dt.
                                     strftime("%A").alias('NameOfDay'))
            return df
        if isinstance(df, pd.DataFrame):
            columns = df.columns.tolist()
            col = ['InvoiceDate', 'Invoice', 'StockCode']

            for col in columns:
                if col == 'InvoiceDate':
                    df = df.rename(columns={'InvoiceDate': 'InvoiceDateTime'})
                    df['InvoiceDateTime'] = pd.to_datetime(
                                                          df['InvoiceDateTime']
                                                          )
                    df['InvoiceDate'] = df['InvoiceDateTime'].dt.date
                    df['InvoiceTime'] = df['InvoiceDateTime'].dt.time
                    df['NameOfDay'] = df['InvoiceDateTime'].dt.day_name()
                if col == 'Invoice':
                    df.loc[:, 'Invoice'] = df.loc[:, 'Invoice'].astype(str)
                if col == 'StockCode':
                    df.loc[:, 'StockCode'] = df.loc[:, 'StockCode'].astype(str)
            return df
    except AttributeError:
        print('\t\tData is not a dataframe.\n\
            Kindly input a dataframe of pandas or polars type.')
        return None


# print()
# if __name__ == '__main__':
    # start = time.time()
    # pl_data = ld.read_polars()
    # # pd_data = ld.read_pandas()

    # # Handle NA
    # pl_df_na_free = pl_na_handler(pl_data)
    # # pd_df_na_free = pd_na_handler(pd_data)

    # # Visualize data
    # viz_data(pl_df_na_free)

    # # Handle Outliers
    # pl_df_clean = handle_outlier_polars(pl_df_na_free, col='Quantity',
    #                                     method='cap')
    # pl_df_clean = handle_outlier_polars(pl_df_na_free, col='Price',
    #                                     method='cap')
    # print(pl_df_clean.describe())

    # # Transform Clean Data
    # pl_df_trans = transform_df(pl_df_clean)
    # print(pl_df_trans.describe())

    # # Visualize data
    # viz_data(pl_df_trans)

    # end = time.time()

    # print(f'Total time {round((end-start), 2)}')

    # start = time.time()
    # pd_data = ld.read_pandas()

    # # Handle NA
    # pd_df_na_free = pd_na_handler(pd_data)

    # # Visualize data
    # viz_data(pd_df_na_free)

    # # Handle Outliers
    # pl_df_clean = handle_outlier_pandas(pd_df_na_free, col='Quantity',
    #                                     method='drop')
    # pl_df_clean = handle_outlier_pandas(pd_df_na_free, col='Price',
    #                                     method='drop')
    # print(pl_df_clean.describe())

    # # Transform Clean Data
    # pl_df_trans = transform_df(pl_df_clean)
    # print(pl_df_trans.describe())

    # # Visualize data
    # viz_data(pl_df_trans, columns=pl_df_trans.columns.tolist())

    # end = time.time()

    # print(f'Total time {round((end-start), 2)}')
