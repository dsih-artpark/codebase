import pandas as pd
import re
from dataio.download import (download_dataset_v2, fetch_file_list, fetch_data_documentation)
import boto3
import datetime
import logging
from epipipeline import get_regionIDs
from epipipeline.preprocess.dengue.karnataka import fetch_ka_summary_v2
from epipipeline.standardise.dengue.karnataka import standardise_ka_summary_v2
import os


# set-up logger
logging.getLogger("epipipeline.standardise.dengue.karnataka.log")

# capture warnings and redirect them to the logging system
logging.captureWarnings(True)

# download the latest regionids file
download_dataset_v2(dsid= "GS0015DS0034")
regionids_df, regionids_dict = get_regionIDs()

# fetch the yaml file and set globals
D = fetch_data_documentation(dsid="EP0006DS0015")
D = D["tables"]["ka-dengue-daily-summary"]
CONFIG = D["admin"]["config"]
SKIP_ROWS = CONFIG["skip_rows"]
COL_INDEX = CONFIG["max_col_index"]
TOTAL_ROW_INDEX = CONFIG["total_row_index"]
TOTAL_COL_INDEX_START = CONFIG["total_col_index_start"]
TOTAL_COL_INDEX_END = CONFIG["total_col_index_end"]
HEADER_MAPPER = CONFIG["header_mapper"]
MIN_COLS = CONFIG["min_cols"]
DATA_DICTIONARY = D["data_dictionary"]
BUCKET_NAME = CONFIG["upload"]["bucket"]
RDS_ID = D["admin"]["dsid"]["raw"]
DSID = D["admin"]["dsid"]["standardised"]

raw_file = "2024-09.xlsx"   # change for latest file
prefix_name = raw_file[:4] # change for latest file
latest_date = "2024-09-18"  # change for latest file

# get the date of the latest file standardised
files = fetch_file_list(dsid=DSID, prefix=prefix_name, data_state="standardised")
L = list(files)
L.sort()
last_file = L[-1]

try:
    last_file_date = pd.to_datetime(last_file)
except Exception as e:
    raise Exception (f"{e} - Invalid file name in the standardised bucket")

raw_data_dict=fetch_ka_summary_v2(raw_file_name=raw_file, 
    raw_folder_prefix=prefix_name, 
    raw_dsid=RDS_ID, 
    latest_std_date=last_file_date,
    max_date=latest_date, 
    skip_rows=SKIP_ROWS,
    total_row_index=TOTAL_ROW_INDEX,
    total_col_index_start=TOTAL_COL_INDEX_START,
    total_col_index_end=TOTAL_COL_INDEX_END
    )

std_dict = standardise_ka_summary_v2(raw_dict = raw_data_dict, 
    drop_cols = COL_INDEX, 
    header_mapper = HEADER_MAPPER, 
    min_cols = MIN_COLS, 
    data_dict = DATA_DICTIONARY, 
    regions=regionids_df)

# Upload to S3
s3 = boto3.resource("s3")
AWS_KEY = DSID+"-"+CONFIG["upload"]["prefix"]+"/"+prefix_name+"/"

for key in std_dict:
    std_df=std_dict[key]
    std_df.to_csv(f"{key}.csv", index=False)

    try:
        s3.meta.client.upload_file(Filename = f"{key}.csv", Bucket= BUCKET_NAME, Key = f"{AWS_KEY}{key}.csv")
        logging.info("Successfully uploaded to AWS S3")
    except Exception as e:
        logging.warning(f"Failed to upload to AWS S3 - {e}")

    os.remove(f"{key}.csv")


###---------------IF CONVERTED FROM PDF, SAVE IT AS AN EXCEL WORKBOOK WITH THE DATE, RUN FUNCTIONS ABOVE---------------###

