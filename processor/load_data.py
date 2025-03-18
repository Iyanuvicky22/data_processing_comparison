"""
Data loading python file.
"""
import time
import pandas as pd
import polars as pl


def read_pandas():
    """
    This function reads the year 2009-2010
    xlxs file using pandas.
    """
    df_pd = pd.read_excel('online_retail_II.xlsx', sheet_name=0)
    return df_pd


def read_polars():
    """
    This function reads the year 2010-2011
    xlxs file using polars.
    """
    df_pl = pl.read_excel('online_retail_II.xlsx', sheet_id=1)
    return df_pl


def compare_time(pd_func, pl_func, action='Loading Time'):
    """
    This function compares loading
    times of the above two functions.
    """
    # Loading time for pandas
    start = time.time()
    pd_func()
    end = time.time()

    # Loading time for polars
    start_2 = time.time()
    pl_func()
    end_2 = time.time()

    res = [f'{action} for pandas is: {round((end-start), 2)}',
           f'{action} for pandas is: {round((end_2-start_2), 2)}',
           ]
    return res


if __name__ == '__main__':
    # compare_time(read_pandas, read_polars)
    ab = read_polars()
    print(ab.head(3))
