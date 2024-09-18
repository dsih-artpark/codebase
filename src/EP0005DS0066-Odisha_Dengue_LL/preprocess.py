import pandas as pd
import re
import dataio.download
from dataio.download import (download_dataset_v2,fetch_data_documentation)
from epipipeline.preprocess import (clean_colname, map_column, extract_test_method_with_result)
import boto3

# set-up metadata from metadata.yaml
metadata=fetch_data_documentation(dsid = "EP0005DS0066")
metadata = metadata["tables"]["odisha_dengue_ll"]
data_dictionary = metadata["data_dictionary"]
config = metadata["admin"]["config"]
rdsid = metadata["admin"]["dsid"]["raw"]
dsid = metadata["admin"]["dsid"]["preprocessed"]
raw_file = config["raw_file"]
skip_rows = config["skip_rows"]
merged_headers = config["merged_headers"]
standard_mapper = config["standard_mapper"]
required_headers = config["required_headers"]

PP_BUCKET_NAME = config["upload"]["bucket"]
FILE_NAME = config["upload"]["file_name"]
KEY = D["admin"]["dsid"]["standardised"]+"-"+CONFIG["upload"]["prefix"]+"/"+FILE_NAME


s3 = boto3.resource("s3")

# download the raw file
raw_excel_path = download_dataset_v2(dsid=rdsid, data_state = "raw")
wb=pd.ExcelFile(f"data/{raw_excel_path}/{raw_file}")

for sheet in wb.sheet_names:
    df=pd.read_excel(wb, sheet_name=sheet, skiprows=skip_rows[sheet])
    # drop cols where all values are null
    df=df.dropna(how="all", axis=1)
    # drop rows where all values are null
    df=df.dropna(how="all", axis=0)
    # deal with merged headers
    ## case 1 - vertical merge - merge upper and lower cols
    if sheet in merged_headers:
        if merged_headers[sheet]==1:
            for i in range(len(df.columns)):
                if not pd.isna(df.iloc[0,i]):
                    df.columns.values[i]=df.iloc[0,i]
            df=df.iloc[1:,:]
    ## case 2 - horizontal merge - fill with col name from previous cell + add index number
        elif config["merged_headers"][sheet]==0:
            for i in range(1, len(df.columns)):
                if re.search("unnamed", str(df.columns[i]), re.IGNORECASE) or pd.isna(df.columns[i]):
                    df.columns.values[i] = df.columns.values[i-1] + "_2"
        else: # no cases of more than one row of merged headers
            pass
    else:
        pass

    # string clean colnames
    headers = [clean_colname(colname=col) for col in df.columns]
    df.columns=headers
    
    # rename columns based on standard mappers
    header_mapper=map_column(map_dict=standard_mapper)
    df=df.rename(columns=header_mapper)


    # extract test results from test cols
    if "test_method" and "result" in df.columns:
        tests = df.apply(lambda x: extract_test_method_with_result(test_method=x["test_method"], result=x["result"]), axis=1)
        df["event.test.test1.result"], df["event.test.test2.result"] = zip(*tests)

    # check that minimum reqd cols are present
    reqd_cols_missing = set(required_headers) - set(df.columns)

    if reqd_cols_missing:
        ## add logger
        print(f"File: {sheet} is missing minimum required columns {reqd_cols_missing}")

    # filter dataset to std names and
    # # drop row where name/address is na (footers)
    df=df.dropna(subset=config["dropna_cols"])

    # remove extraneous cols
    for col in df.columns:
        if col not in data_dictionary.keys():
            df=df.drop(columns=col)

    # upload to preprocessed
    df.to_csv(f"{sheet}.csv", index=False)

   
try:
    s3.meta.client.upload_file(Filename = FILE_NAME, Bucket= BUCKET_NAME, Key = KEY)
    logging.info("Successfully uploaded to AWS S3")
except Exception as e:
    logging.warning(f"Failed to upload to AWS S3 - {e}")


