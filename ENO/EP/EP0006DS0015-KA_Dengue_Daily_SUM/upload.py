import pandas as pd
import re
import os
import boto3
client = boto3.client('s3')

## -----------------------------UPLOAD-------------------------------------- ##

# Steps:
## 1. Download daily report excel workbook received from department via email
## 2. Pass file path and report date (mentioned in email) into the extract_raw_summary function below 
## which will extract the sheet and push it to the raw bucket on AWS

def upload_raw_summary(raw_file_path, report_date: str) -> str:

    """Extracts daily summary for specified report date from the workbook shared by GoK, and uploads to AWS S3

    Raises:
        Exception: Unable to locate raw file through raw_file_path provided
        Exception: Unable to find sheet for report_date specified
        Exception: Raw file already exists in AWS S3
        Exception: Failed to upload file to AWS S3

    Returns:
        str : Success/Failure message for upload
    """

    assert re.match(r"\d{4}\-\d{2}\-\d{2}", report_date), "Invalid report date, enter as yyyy-mm-dd"

    if os.path.exists(raw_file_path):
        filename=f"wb_{report_date}.xlsx"
        os.rename(raw_file_path, filename)
    else:
        raise Exception("Unable to locate raw file")

    # extract date from filename
    date_pattern=re.search(r"(\d{4}\-\d{2}\-\d{2})(.xlsx)", filename)
    if date_pattern:
        if date_pattern.group(1):
            date=pd.to_datetime(date_pattern.group(1))
            year=date.strftime("%y")
            # assuming sheet names are in the format 4-2-24 (d-m-yy)
            sheet_pattern=f"DDR {date.day}-{date.month}-{year}"
        else:
            raise Exception("Failed: Sheet not found for date.")
    else:
        raise Exception("Failed: Sheet not found for date.")

    # open workbook, and locate sheet based on date
    workbook=pd.ExcelFile(filename)
    found=False
    for sheet in workbook.sheet_names:
        if re.search(sheet_pattern, sheet):
            df=pd.read_excel(workbook, sheet_name=sheet)
            RAW_FILENAME=f'{date.strftime("%Y")}-{date.strftime("%m")}-{date.strftime("%d")}.xlsx'
            df.to_excel(f"{RAW_FILENAME}", index=False)
            found=True
            break
    if not found:
        raise Exception("Failed: Sheet not found for date.")

    # boto3 upload raw to aws

    response=client.list_objects_v2(Bucket='dsih-artpark-01-raw-data',Prefix=f'EPRDS8-KA_Dengue_Chikungunya_SUM/Daily/{year}/By_Day/')

    if 'Contents' in response:
        raise Exception("Failed: File already exists in S3")
    else:
        try:
            client.upload_file(Filename=RAW_FILENAME, Bucket='dsih-artpark-01-raw-data', Key=f'EPRDS8-KA_Dengue_Chikungunya_SUM/Daily/{year}/By_Day/{RAW_FILENAME}')
        except Exception as e:
            return(f"Failed: Unable to upload to S3: {e}")
    
    os.remove(RAW_FILENAME)
    os.remove(filename)
    return ("Success: Raw file uploaded to S3")

# sample input
upload_raw_summary("wb_2024-05-13.xlsx")

