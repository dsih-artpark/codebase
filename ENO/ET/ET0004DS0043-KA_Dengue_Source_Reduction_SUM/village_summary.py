import pandas as pd
import os
import re
import yaml
from fuzzywuzzy import process
import numpy as np

from functions import *


# SETTING UP CONFIG
with open("metadata.yaml", "r") as f:
    file=yaml.safe_load(f)
    f.close()

D=file["tables"]["village_summary"]

column_map=D["column_mapping"]
master_colval=D["column_values"]
master_cols=list(D["column_values"].keys())

# FILE CONSOLIDATION

files=[file for file in os.listdir("./data/") if file.endswith(".csv")]

main_df=pd.DataFrame()

for file in files:
    df=pd.read_csv(f"./data/{file}")
    for col in df.columns:
        if re.search(r"unnamed", col, re.IGNORECASE):
            df.drop(columns=[col], inplace=True)
        if re.search(r"^\d", col):
            df.rename(columns={col:"reportingperiod"}, inplace=True)
    new_cols=[]
    for col in df.columns:
        newcol=map_columns(col, column_map)
        new_cols.append(newcol)

    assert len(df.columns)==len(new_cols)
    df.columns=new_cols
    main_df=main_df._append(df)

for col in master_cols:
    if col not in main_df.columns:
        main_df[col]=master_colval[col]
main_df=main_df[master_cols]

# DROP INVALID ROWS
main_df.dropna(subset=["location.admin5.name", "survey.housesVisited"], inplace=True)
main_df=main_df[main_df["location.admin2.name"]!="Total"]

# GEOMAPPING
regions=pd.read_csv("regionids.csv")

# some manual cleaning - dist & subdist mapping

main_df.loc[main_df["location.admin2.name"]=="BBMP", "location.admin3.name"]="BBMP"

# Map district name to standardised LGD name and code
dists=main_df.apply(lambda x: dist_mapping(x["location.admin1.ID"], x["location.admin2.name"], regions, 65), axis=1)
main_df["location.admin2.name"], main_df["location.admin2.ID"]=zip(*dists)

main_df[main_df["location.admin2.ID"]=="admin_0"]


# Map subdistrict/ulb name to standardised LGD name and code
subdist=main_df.apply(lambda x: subdist_ulb_mapping(x["location.admin2.ID"], x["location.admin3.name"], regions, 
65), axis=1)
main_df["location.admin3.name"], main_df["location.admin3.ID"]=zip(*subdist)

# Map village/ward name to standardised LGD name and code
villages=main_df.apply(lambda x: village_ward_mapping(x["location.admin3.ID"], x["location.admin5.name"], regions, 65 ), axis=1)
main_df["location.admin5.name"], main_df["location.admin5.ID"]=zip(*villages)

# Extract admin hierarchy from admin3.ID - ULB, REVENUE, admin_0 (if missing ulb/subdistrict LGD code)
main_df["location.admin.hierarchy"]=main_df["location.admin3.ID"].apply(lambda x: "ULB" if x.startswith("ulb") else ("REVENUE" if x.startswith("subdistrict") else "admin_0"))

# Drop duplicates across all vars after standardisation
main_df.drop_duplicates(inplace=True)

# Fixing dates
main_df["metadata.reportPeriod"]=main_df["metadata.reportPeriod"].str.replace(".","-")
main_df["metadata.reportPeriod"]=main_df["metadata.reportPeriod"].str.split("to")


def date_time_set(x: list) -> tuple:
    import datetime
    print(type(x))
    start_date=x[0]
    end_date=x[1]
    date1=datetime.date(day=int(start_date[:2]), month=int(start_date[3:5]), year=int(start_date[6:10])).isoformat()
    date2=datetime.date(day=int(end_date[:2]), month=int(end_date[3:5]), year=int(end_date[6:10])).isoformat()
    return([date1, date2], end_date)

dates=main_df["metadata.reportPeriod"].apply(lambda x: date_time_set(x))
main_df["metadata.reportPeriod"], main_df["metadata.primaryDate"]=zip(*dates)


# standardise dtypes
def NumvarStd(x):
    """_summary_

    Args:
        x (_type_): string/object variable

    Returns:
        _type_: numbers
    """
    if re.search(r"[^\d]", str(x)):
        res=re.search(r"\d+", str(x))
        if res:
            return res.group(0)
        else:
            return np.nan
    else:
        return x

# fixing dtypes
for col in main_df.columns:
    if col.startswith("summary") or col.startswith("survey"):
        if main_df[col].dtype=="object":
            main_df[col]=main_df[col].apply(lambda x: NumvarStd(x))
            main_df[col]=main_df[col].astype(float)

for col in df.columns:
    if col.startswith("survey"):
        print(col, df[col].isna().sum())


main_df["survey.housesPositive"].fillna(0, inplace=True)
main_df["survey.containersSearched"].fillna(0, inplace=True)
main_df["survey.containersSearched"].fillna(0, inplace=True)


# adding calc variables
main_df["survey.houseIndex.calc"]=(main_df["survey.housesPositive"]/main_df["survey.housesVisited"]) * 100
main_df["survey.containerIndex.calc"]=(main_df["survey.containersPositive"]/main_df["survey.containersSearched"]) * 100
main_df["survey.breteauIndex.calc"]=(main_df["survey.containersPositive"]/main_df["survey.housesVisited"]) * 100

# nullifying invalid 0's
main_df.loc[main_df["survey.housesVisited"].isna(),"survey.houseIndex"]=np.nan
main_df.loc[main_df["survey.containersSearched"].isna(),"survey.containerIndex"]=np.nan
main_df.loc[main_df["survey.housesVisited"].isna(),"survey.breteauIndex"]=np.nan

# rounding-up int cols
int_cols=['summary.noOfHouses', 'survey.housesVisited', 'survey.housesPositive','survey.containersSearched', 'survey.containersPositive', 'survey.containersReduced']

for col in int_cols:
    main_df[col]=main_df[col].round(0)


float_cols=['survey.houseIndex', 'survey.containerIndex', 'survey.breteauIndex', 'survey.houseIndex.calc', 'survey.containerIndex.calc', 'survey.breteauIndex.calc']

for col in float_cols:
    main_df[col]=main_df[col].round(2)

# creating uuids
import uuid
main_df["metadata.recordID"]=[uuid.uuid4() for i in range(len(main_df))]

main_df.to_csv("source-reduction-dist.csv", index=False)

main_df=pd.read_csv("source-reduction-dist.csv")
df=pd.read_csv("source-reduction-hassan.csv")

main_df=main_df._append(df)

main_df["metadata.primaryDate"]=pd.to_datetime(main_df["metadata.primaryDate"], format="mixed").dt.strftime('%Y-%m-%dT%H:%M:%SZ')

for col in ['location.admin2.name',  'location.admin3.name','location.admin5.name', 'location.healthcentre.phc','location.healthcentre.subcentre']:
    main_df[col]=main_df[col].str.upper().str.strip()

main_df=main_df.drop_duplicates()

main_df.to_csv("source-reduction-dist.csv", index=False)