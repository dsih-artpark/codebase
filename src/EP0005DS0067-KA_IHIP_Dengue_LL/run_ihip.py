from epipipeline.preprocess.dengue.ihip import (fetch_ihip_v2, preprocess_ihip_v2)
from epipipeline.standardise.dengue.ihip import (standardise_ihip_v2)
from epipipeline import get_regionIDs
from dataio.download import fetch_data_documentation, download_dataset_v2
import pandas as pd
import boto3
import os


# set-up logging
epipipeline.logging_config.setup_logging('DEBUG')
dataio.logging_config.setup_logging('INFO')

# download the latest regionids file

download_dataset_v2(dsid= "GS0015DS0034")
regionids_df, regionids_dict = get_regionIDs()

# Fetch metadata.yaml
D = fetch_data_documentation(dsid="EP0005DS0067")
D = D["tables"]["ka_ihip"]
CONFIG = D["admin"]["config"]
GSHEET_LINK = CONFIG["gsheet"]
EXPECTED_SHEETS = CONFIG["expected_sheets"]
CREDENTIALS_PATH = CONFIG["credentials"]
HEADER_MAPPER = CONFIG["header_mapper"]
MIN_COLS = CONFIG["min_cols"]
DATA_DICTIONARY = D["data_dictionary"]
STR_COLS = CONFIG["str_cols"]
DATE_VARS = CONFIG["date_vars"]
ID_VARS = CONFIG["id_vars"]
GEO_VARS = CONFIG["geo_vars"]

BUCKET_NAME = CONFIG["upload"]["bucket"]
FILE_NAME = CONFIG["upload"]["file_name"]
KEY = D["admin"]["dsid"]["standardised"]+"-"+CONFIG["upload"]["prefix"]+"/"+FILE_NAME

# Fetch raw data for GSHEET
raw_dict = fetch_ihip_v2(json_cred = CREDENTIALS_PATH, gsheet = GSHEET_LINK, raw_sheets = EXPECTED_SHEETS)

# Preprocessed data 
preprocessed_dict = preprocess_ihip_v2(raw_data_dict=raw_dict, standard_mapper=HEADER_MAPPER, minimum_columns=MIN_COLS)

# Standardised dict
standardised_dict = standardise_ihip_v2(preprocessed_data_dict=preprocessed_dict, data_dictionary=DATA_DICTIONARY, date_vars = DATE_VARS, id_vars = ID_VARS, str_vars = STR_COLS, geo_vars = GEO_VARS, limit_year = "2024", regions=regionids_df)

data = pd.concat(standardised_dict.values(), ignore_index=True)

data.to_csv(FILE_NAME, index=False)

# Upload to S3
s3 = boto3.resource("s3")
try:
    s3.meta.client.upload_file(Filename = FILE_NAME, Bucket= BUCKET_NAME, Key = KEY)
    logging.info("Successfully uploaded to AWS S3")
except Exception as e:
    logging.warning(f"Failed to upload to AWS S3 - {e}")


# Delete csv
os.remove(FILE_NAME)

