import pandas as pd
import re
import os
from fuzzywuzzy import process
import yaml
import datetime
import boto3
from functions import *

client = boto3.client('s3')

## -----------------------------SETTING GLOBALS-------------------------------- ##

with open("metadata.yaml") as f:
    D=yaml.safe_load(f)

COLUMN_MAP=D["column_mapping"]["current"]
COLUMN_VALUES=D["column_values"]["current"]
COLUMN_MASTER=list(COLUMN_VALUES.keys())
SKIP=D["config"]["skip"]
COLS=D["config"]["cols"]
MIN_COLS=D["config"]["min_cols"]
THRESHOLDS=D["config"]["thresholds"]

client.download_file(Bucket='dsih-artpark-03-standardised-data', Key='GS0015DS0034-LGD_Region_IDs_and_Names/regionids.csv', Filename='regionids.csv')
regions=pd.read_csv("regionids.csv")


## -----------------------------PREPROCESS-------------------------------------- ##

def standardise(raw_file_date):
    
    assert re.match(r"\d{4}\-\d{2}\-\d{2}", raw_file_date), "Invalid filename, enter as yyyy-mm-dd"

    date=pd.to_datetime(raw_file_date, format="%Y-%m-%d")
    year=date.year

    # downloading the raw file
    try:
        client.download_file(Bucket='dsih-artpark-01-raw-data', Key=f'EPRDS8-KA_Dengue_Chikungunya_SUM/Daily/{year}/By_Day/{raw_file_date}.xlsx', Filename=f'{raw_file_date}.xlsx')
    except Exception:
        print("Raw file not found on AWS S3")

    df=pd.read_excel(f'{raw_file_date}.xlsx', skiprows=SKIP)

    # drop extraneous cols (set in metadata.yaml)
    df=df.iloc[:,:COLS]

    #forward fill unnamed and nan in current columns
    for i in range(1,len(df.columns)):
        if (re.search("Unnamed", str(df.columns[i]), re.IGNORECASE)) or (re.search("NaN", str(df.columns[i]), re.IGNORECASE)):
            df.columns.values[i]=df.columns.values[i-1]
            
    # identify index where df starts - i.e., S.No. is 1 - not ideal, explore pivot column
    df_start=df[df.iloc[:,0]==1].index[0]

    # for each header row in the dataframe (except last), forward fill if nan
    for row in range(df_start-1):
        df.iloc[row]=df.iloc[row].ffill()

    # for each header row in the dataframe,upward merge
    for row in range(df_start):
        row_data=df.iloc[row].to_list()
        for i in range(len(row_data)):
            if not re.search("nan", str(row_data[i]), re.IGNORECASE):
                df.columns.values[i]=re.sub(r"[\d\-\(\)\s]+", "", df.columns.values[i].strip())+"_"+ re.sub(r"[\d\-\(\)\s]+", "", str(row_data[i]).strip())
    

    # drop village, etc.
    drop_cols=[col for col in df.columns if re.search(r"Taluk|Village|PHC|Population|Block|Remarks", col, re.IGNORECASE)]
    df.drop(columns=drop_cols, inplace=True)
    # remove header rows
    df=df.iloc[df_start:,:]
    # map cols
    df.columns=[map_columns(col, COLUMN_MAP) for col in df.columns]
    # check that min cols are present
    if not set(MIN_COLS).issubset(set(df.columns)):
        raise Exception(f"File is missing minimum required columns - {set(MIN_COLS).difference(set(df.columns))}")

    # add standardised cols from metadata.yaml
    # adding standard list of columns from metadata that are not present in the dataset
    for col in COLUMN_MASTER:
        if col not in df.columns:
            df[col]=COLUMN_VALUES[col]["value"]

    # extract BBMP from S.No. to district - the district 
    df["sl_no"]=df["sl_no"].astype(str)
    df.loc[(df["sl_no"].str.contains(r"[Cc]ity")==True), "location.admin3.name"]="BBMP"
    df.loc[(df["location.admin3.name"]=="BBMP"), "location.admin2.name"]="BENGALURU URBAN"
            
    # drop total, rows with district name missing
    df=df[(df["location.admin2.name"].str.contains(r"[Tt]otal")==False) & (df["sl_no"].str.contains(r"[Tt]otal")==False) & (df["location.admin2.name"].isna()==False)]
    
    # filtering dataset to retain only standardised cols
    df=df[COLUMN_MASTER]
    
    # geo-mapping - districts
    # Map district name to standardised LGD name and code
    dists=df.apply(lambda x: dist_mapping(x["location.admin1.ID"], x["location.admin2.name"], regions, 
    THRESHOLDS["district"]), axis=1)
    df["location.admin2.name"], df["location.admin2.ID"]=zip(*dists)

    assert len(df[df["location.admin2.ID"]=="admin_0"])==0, "District(s) missing"

    # Map subdistrict/ulb name to standardised LGD name and code
    subdist=df.apply(lambda x: subdist_ulb_mapping(x["location.admin2.ID"], x["location.admin3.name"], regions, 
    THRESHOLDS["subdistrict"]), axis=1)
    df["location.admin3.name"], df["location.admin3.ID"]=zip(*subdist)

    # Extract admin hierarchy from admin3.ID - ULB, REVENUE, admin_0 (if missing ulb/subdistrict LGD code)
    df["location.admin.hierarchy"]=df["location.admin3.ID"].apply(lambda x: "ULB" if x.startswith("ulb") else ("REVENUE" if x.startswith("subdistrict") else "admin_0"))

    # Drop duplicates across all vars after standardisation
    df.drop_duplicates(inplace=True)

    # Generate recordID after standardisation and de-duplication
    df["metadata.recordID"]=[uuid.uuid4() for i in range(len(df))]

    # Generate recordDate from filename
    df["metadata.recordDate"]=date.strftime('%Y-%m-%dT%H:%M:%SZ')
    df["metadata.ISOWeek"]=date.isocalendar().week

    # Cleaning int cols
    for col in df.columns:
        if col.startswith("daily") or col.startswith("cumulative"):
            df[col]=df[col].fillna(0)
            df[col]=df[col].astype(int)

    # Merge with standardised dataset
    
    client.download_file(Bucket='dsih-artpark-03-standardised-data', Key='EP0006DS0015-KA_Dengue_Daily_SUM/2024.csv', Filename=f'{year}.csv')

    main_df=pd.read_csv(f'{year}.csv')

    if len(main_df[main_df["metadata.recordDate"]==date])==0:
        main_df=main_df._append(df)
        main_df.to_csv(f'{year}.csv', index=False)

        main_df
        try:
            client.upload_file(Filename=f'{year}.csv', Bucket='dsih-artpark-03-standardised-data', Key=f'EP0006DS0015-KA_Dengue_Daily_SUM/{year}.csv')
        except Exception as e:
            return(f"Failed: Unable to upload to S3: {e}")
    else:
        raise("Duplicate Alert: Date already exists in standardised data")
    
    os.remove(f'{raw_file_date}.csv')
    os.remove(f'{year}.csv')
    return ("Success: Standardised file uploaded to S3")

# sample input
standardise("2024-05-13")







