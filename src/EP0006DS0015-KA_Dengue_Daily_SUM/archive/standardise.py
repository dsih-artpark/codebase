import pandas as pd
import re
import os
from fuzzywuzzy import process
import yaml
import datetime
import boto3
from epipipeline import get_regionIDs
from epipipeline.standardise import *
from epipipeline.preprocess import *
from epipipeline.standardise.gis import *
import uuid
from dataio.download import fetch_data_documentation
# client = boto3.client('s3')

## -----------------------------SETTING GLOBALS-------------------------------- ##

D = fetch_data_documentation(dsid="EP0006DS0015")

# with open("metadata.yaml") as f:
#     D=yaml.safe_load(f)


COLUMN_MAP = D["tables"]["ka-dengue-daily-summary"]["config"]["column_mapping"]
COLUMN_VALUES = D["tables"]["ka-dengue-daily-summary"]["data_dictionary"]
COLUMN_MASTER = list(COLUMN_VALUES.keys())

SKIP = D["tables"]["ka-dengue-daily-summary"]["config"]["skip_rows"]

COLS = D["tables"]["ka-dengue-daily-summary"]["config"]["max_col_index"]
MIN_COLS = D["tables"]["ka-dengue-daily-summary"]["config"]["min_cols"]
THRESHOLDS = D["tables"]["ka-dengue-daily-summary"]["config"]["thresholds"]

raw_file_name = "2024-08-07.xlsx"


## -----------------------------PREPROCESS-------------------------------------- ##

def standardise(raw_file_name):

    assert re.match(r"\d{4}\-\d{2}\-\d{2}.xlsx",raw_file_name), "Invalid filename, enter as yyyy-mm-dd.xlsx"

    date = pd.to_datetime(raw_file_name.split(".")[0], format="%Y-%m-%d")
    year = date.year

    df = pd.read_excel(f'{raw_file_name}', skiprows=SKIP)

    # drop extraneous cols (set in metadata.yaml)
    df = df.iloc[:, :COLS]

    # forward fill unnamed and nan in current columns
    for i in range(1, len(df.columns)):
        if (re.search("Unnamed", str(df.columns[i]), re.IGNORECASE)) or (re.search("NaN", str(df.columns[i]), re.IGNORECASE)):
            df.columns.values[i] = df.columns.values[i-1]


    # identify index where df starts - i.e., S.No. is 1 - not ideal, explore pivot column
    df_start = df[df.iloc[:, 0] == 1].index[0]

    # for each header row in the dataframe (except last), forward fill if nan
    for row in range(df_start-1):
        df.iloc[row] = df.iloc[row].ffill()

    # for each header row in the dataframe,upward merge
    for row in range(df_start):
        row_data = df.iloc[row].to_list()
        for i in range(len(row_data)):
            if not re.search("nan", str(row_data[i]), re.IGNORECASE):
                df.columns.values[i] = re.sub(r"[\d\-\(\)\s]+", "", df.columns.values[i].strip(
                ))+"_" + re.sub(r"[\d\-\(\)\s]+", "", str(row_data[i]).strip())

    # drop village, etc.
    drop_cols = [col for col in df.columns if re.search(
        r"Taluk|Village|PHC|Population|Block|Remarks", col, re.IGNORECASE)]
    df.drop(columns=drop_cols, inplace=True)

    # remove header rows
    df = df.iloc[df_start:, :]

    # map cols
    headers=[clean_colname(colname=col) for col in df.columns]
    df.columns=headers

    standard_headers = map_column(map_dict=COLUMN_MAP)

    df.rename(columns=standard_headers, inplace=True)

    # check that min cols are present
    if not set(MIN_COLS).issubset(set(df.columns)):
        raise Exception(f"File is missing minimum required columns - {set(MIN_COLS).difference(set(df.columns))}. Current columns are: {df.columns}")



    # add standardised cols from metadata.yaml
    # adding standard list of columns from metadata that are not present in the dataset
    for col in COLUMN_MASTER:
        if col not in df.columns:
            if "default_value" in COLUMN_VALUES[col]:
                df[col] = COLUMN_VALUES[col]["default_value"]
            else:
                df[col] = pd.NA

    # extract BBMP from S.No. to district - the district
    df["sl_no"] = df["sl_no"].astype(str)
    df.loc[(df["sl_no"].str.contains(r"[Cc]ity") == True),
           "location.admin3.name"] = "BBMP"
    df.loc[(df["location.admin3.name"] == "BBMP"),
           "location.admin2.name"] = "Bengaluru Urban"

    # drop total, rows with district name missing
    df = df[(df["location.admin2.name"].str.contains(r"[Tt]otal") == False) & (
        df["sl_no"].str.contains(r"[Tt]otal") == False) & (df["location.admin2.name"].isna() == False)]

    # filtering dataset to retain only standardised cols
    df = df[COLUMN_MASTER]

    # geo-mapping - districts
    # Map district name to standardised LGD name and code
    dists = df.apply(lambda x: dist_mapping(stateID=x["location.admin1.ID"], districtName=x["location.admin2.name"], df=regionids_df,
                                            threshold=THRESHOLDS["district"]), axis=1)

    df["location.admin2.name"], df["location.admin2.ID"] = zip(*dists)

    assert len(df[df["location.admin2.ID"] == "admin_0"]
               ) == 0, "District(s) missing"

    # Map subdistrict/ulb name to standardised LGD name and code

    subdist = df.apply(lambda x: subdist_ulb_mapping(districtID=x["location.admin2.ID"], subdistName=x["location.admin3.name"], df=regionids_df,
                                                     threshold=THRESHOLDS["subdistrict"]), axis=1)
    df["location.admin3.name"], df["location.admin3.ID"] = zip(*subdist)

    # Extract admin hierarchy from admin3.ID - ULB, REVENUE, admin_0 (if missing ulb/subdistrict LGD code)
    df["location.admin.hierarchy"] = df["location.admin3.ID"].apply(lambda x: pd.NA if pd.isna(
        x) else "ULB" if x.startswith("ulb") else "Revenue" if x.startswith("subdistrict") else "admin_0")

    # Drop duplicates across all vars after standardisation
    df.drop_duplicates(inplace=True)

    # Generate recordID after standardisation and de-duplication
    df["metadata.recordID"] = [uuid.uuid4() for i in range(len(df))]

    # Generate recordDate from filename
    df["metadata.recordDate"] = date.strftime('%Y-%m-%dT%H:%M:%SZ')
    df["metadata.ISOWeek"] = date.isocalendar().week

    # Cleaning int cols
    for col in df.columns:
        if col.startswith("daily") or col.startswith("cumulative"):
            df[col] = df[col].fillna(0)
            df[col] = df[col].astype(int)

    # export file
    df.to_csv(f"{raw_file_name.split('.')[0]}.csv", index=False)

    return df


# # sample input

for file in os.listdir():
    if file.endswith(".xlsx"):
        standardise(file)
