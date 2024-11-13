import dataio
import epipipeline
import pandas as pd
from dataio.download import download_dataset_v2, fetch_data_documentation
from epipipeline import get_regionIDs
from epipipeline.preprocess.dengue.karnataka import fetch_ka_linelist_v2, preprocess_ka_linelist_v2
from epipipeline.standardise.dengue.karnataka import standardise_ka_linelist_v3
import boto3
import logging
import os

epipipeline.logging_config.setup_logging('DEBUG')
dataio.logging_config.setup_logging('INFO')

year = 2024

metadata = fetch_data_documentation(dsid = "EP0005DS0014", default=True, repo_info = {"branch": "production"})

metadata = metadata["tables"]["ka_dengue_line_lists"]

data_dictionary = metadata["data_dictionary"]
admin = metadata["admin"]
config = admin["config"]

bucket_name = admin["upload"]["bucket"]
file_name = f"{year}.csv"
key = admin["dsid"]["standardised"]+"-"+admin["upload"]["prefix"]+"/"+file_name

# Ensure RegionIDs.csv is available locally
download_dataset_v2(dsid="GS0015DS0034")
regionIDs_df, regionIDs_dict = get_regionIDs()

raw_data_dict = fetch_ka_linelist_v2(dsid=admin["dsid"]["raw"],
                                     sheet_codes=config["sheet_codes"],
                                     regionIDs_dict=regionIDs_dict,
                                     expected_files=config["expected_files"][year],
                                     year=year)


# Live Header Mapper is the mapper for live data, post 2023
live_header_mapper = config["header_mapper"]["live"]
ffill_cols_dict = config["ffill_cols"]
date_cols = config["date_cols"]

no_merge_headers = live_header_mapper["no_merge_headers"]
district_specific_errors = live_header_mapper["district_specific_errors"]
standard_mapper = live_header_mapper["standard_mapper"]
required_headers = live_header_mapper["required_headers"]

preprocessed_data_dict = preprocess_ka_linelist_v2(raw_data_dict=raw_data_dict,
                                                   regionIDs_dict=regionIDs_dict,
                                                   no_merge_headers=no_merge_headers,
                                                   district_specific_errors=district_specific_errors,
                                                   standard_mapper=standard_mapper,
                                                   required_headers=required_headers,
                                                   date_col_names=date_cols,
                                                   ffill_cols_dict=ffill_cols_dict)

THRESHOLDS = {
    "subdistrict": 65,
    "village": 95
}

standardised_data_dict = standardise_ka_linelist_v3(preprocessed_data_dict=preprocessed_data_dict,
                                                    THRESHOLDS=THRESHOLDS,
                                                    STR_VARS=config["str_cols"],
                                                    regionIDs_df=regionIDs_df,
                                                    regionIDs_dict=regionIDs_dict,
                                                    data_dictionary=data_dictionary
                                                    )

df = pd.concat(standardised_data_dict.values(), ignore_index=True)
df.to_csv(f"{year}.csv", index=False)


# Upload to S3
s3 = boto3.resource("s3")
try:
    s3.meta.client.upload_file(Filename = file_name, Bucket= bucket_name, Key= key)
    logging.info("Successfully uploaded to AWS S3. Deleting file...")
except Exception as e:
    logging.warning(f"Failed to upload to AWS S3 - {e}")

os.remove(file_name)



