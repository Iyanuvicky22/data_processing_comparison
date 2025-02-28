"""
FastAPI Application Module
"""
import json
import os
from fastapi import FastAPI
from processor import load_data as ld
from processor import clean as cl
from processor import aggregate as ag
from fastapi.responses import FileResponse

app = FastAPI()

raw_pandas_df = None
raw_polars_df = None

clean_pandas_df = None
clean_polars_df = None

@app.get('/home')
def home():
    """
    Home Function
    """
    return '''Welcome to: Data processing packages
              comparison (pandas vs polars) API project'''


def data_loading():
    """
    API call to load datasets.
    """
    global raw_pandas_df, raw_polars_df
    if raw_pandas_df is None:
        pd_df = ld.read_pandas()
        raw_pandas_df = pd_df
    if raw_polars_df is None:
        pl_df = ld.read_polars()
        raw_polars_df = pl_df


@app.get('/Data Processing')
async def processing():
    """
    API call to process the data sets.
    """
    global raw_pandas_df, raw_polars_df, \
        clean_pandas_df, clean_polars_df

    try:
        data_loading()
        # Pandas Cleaning
        pd_na_free = cl.pd_na_handler(raw_pandas_df)  # Handle NA
        cl.viz_data(pd_na_free)  # Visualize data
        pd_df_clean = cl.handle_outlier_pandas(pd_na_free, col='Quantity',
                                               method='cap')
        pd_df_clean = cl.handle_outlier_pandas(pd_na_free, col='Price',
                                               method='cap')  # Handle Outliers
        pd_df_trans = cl.transform_df(pd_df_clean)  # Transform Clean Data
        cl.viz_data(pd_df_trans)  # Visualize data
        clean_pandas_df = pd_df_trans

        # Pandas Aggregating
        pandas_aggregate = ag.aggregate_pandas(clean_pandas_df)

        # Write Pandas to Json
        json_pandas = pandas_aggregate.to_json(orient='records')


        # Polars
        pl_na_free = cl.pl_na_handler(raw_polars_df)  # Handle NA
        cl.viz_data(pl_na_free)  # Visualize data
        pl_df_clean = cl.handle_outlier_polars(pl_na_free, col='Quantity',
                                               method='cap')
        pl_df_clean = cl.handle_outlier_polars(pl_na_free, col='Price',
                                               method='cap')  # Handle Outliers
        pl_df_trans = cl.transform_df(pl_df_clean)  # Transform Clean Data
        cl.viz_data(pl_df_trans)  # Visualize data
        clean_polars_df = pl_df_trans

        # Pandas Aggregating
        polars_aggregate = ag.aggregate_polars(clean_polars_df)

        # Write Polars to Json
        json_polars = polars_aggregate.write_json()

        return {
            'message': 'Data Processing Results',
            'success': True,
            'data': {
                'pandas': json.loads(json_pandas),
                'polars': json.loads(json_polars)
            }
        }
    except Exception as e:
        print(f'Error Returned: {e}')
        return {
            'message': 'Data Processing Results',
            'success': False,
            'next step': f'Resolve error {e}'
        }


@app.get('/Time Comparison')
async def time_compare():
    """
    This function show the time comparison
    between pandas and polars.
    """
    data_loading()
    processing()

    # Loading time comparison
    ld.compare_time(ld.read_pandas, ld.read_polars)

    # Aggregation time comparison
    ld.compare_time(ag.aggregate_pandas(clean_pandas_df),
                    clean_polars_df,
                    action='Aggregation Time')


@app.get("/download-json")
def download_json():
    """
    Download the processed data as a JSON file
    """
    try:
        file_path = "pandas_data.json"

        # check if the file exists
        if not os.path.exists(file_path):
            return f"File not found: {file_path}"
    except Exception as e:
        return f"Error while downloading file: {e}"

    return FileResponse(file_path, media_type="application/json",
                        filename="pandas_data.json")  


@app.get("/download-parquet")
def download_parquet():
    """
    Download the processed data as a Parquet file
    """
    try:
        file_path = "pandas_data.parquet"

        # check if the file exists
        if not os.path.exists(file_path):
            return f"File not found: {file_path}"
    except Exception as e:
        return f"Error while downloading file: {e}"  
    return FileResponse(file_path, media_type="application/octet-stream",
                        filename="pandas_data.parquet")

