import os
import pandas as pd
import re
import numpy as np
import yaml
from fuzzywuzzy import process
import datetime
import uuid


# 1.0 - FILES CONSOLIDATION

years=["2021", "2022", "2023"]

main_df=pd.DataFrame()

for year in years:
    for file in os.listdir(f"{year}"):
        if re.search(r"summary", file, re.IGNORECASE):
            df=pd.read_csv(f"./{year}/{file}")
            # drop extra columns 
            for col in df.columns:
                if re.search(r"unnamed", col, re.IGNORECASE):
                    df.drop(columns=col, inplace=True)
                   
            # strip symbols, left/right spaces
            df.columns=[re.sub(r"[^\w\d\s]", "", col.lstrip().rstrip()) for col in df.columns]
    
            # extract date from filename
            df["dateRange"]=re.search(r"(\d+.\d+)", file).group(0)

            main_df=main_df._append(df)

            print(df.columns)
    path=f"./clean"

# export
if not os.path.exists(path):
    os.mkdir(path)
main_df.to_csv(f"{path}/consolidated_summary(2021-2023).csv", index=False)

# 02 - DATA CLEANING
main_df=pd.read_csv("./clean/consolidated_summary(2021-2023).csv")

# LOADING CONFIG FILE
with open("metadata.yaml", "r") as f:
    D=yaml.safe_load(f)


# renaming preprocessed columns to their standardised names
preprocessed_col_map=D["column_mapping"]


def MapColumns(colname:str, map_dict: dict) -> str: 
    """_summary_

    Args:
        colname (str): Current column in DataFrame
        map (dict): Dictionary mapping of preprocessed col names to standardised col names

    Returns:
        str: Standardised column name
    """

    for key, values in map_dict.items():
        if colname in values:
            return key
    return colname

main_df.columns=[MapColumns(col, preprocessed_col_map) for col in main_df.columns]

# drop null rows & totals
main_df.dropna(subset="location.district.name", inplace=True)
main_df=main_df[main_df["location.district.name"]!="Total"]


# Standardising districts
regions=pd.read_csv("regionids.csv")
regions=regions[regions["parentID"]==D["column_values"]["location.state.ID"]]

dist_map=dict()
for index, row in regions.iterrows():
    dist_map[row["regionName"]]=row["regionID"]

score_config=D["config"]["district_fuzzymatch"]

def DistrictMatch(x:str, districts:dict=dist_map, score=score_config)-> tuple:
    """_summary_

    Args:
        x (str): District Name in the DataFrame
        districts (dict, optional): Dictionary mapping district names to district IDs, created from regions.csv 

    Returns:
        tuple: _description_
    """
    result=process.extractOne(x, districts.keys(), score_cutoff=score_config)

    if result:
        return result[0], districts[result[0]]
    else:
        return x, np.nan

main_df["location.admin.hierarchy"]=""
main_df["location.ulb.ID"]=""
main_df["location.ulb.name"]=""
main_df["location.district.name"]=main_df["location.district.name"].apply(lambda x: re.sub(r"[^\w\s]","", x.lstrip().rstrip().upper()))
main_df["location.district.name"]=main_df["location.district.name"].apply(lambda x: re.sub(r"[^\w\s]","", x.lstrip().rstrip().upper()))
main_df.loc[main_df["location.district.name"]=="BBMP", "location.ulb.name"]="BBMP"
main_df.loc[main_df["location.district.name"]=="BBMP", "location.ulb.ID"]="ulb_276600"
main_df.loc[main_df["location.district.name"]=="BBMP", "location.district.name"]="BENGALURU URBAN"

match=main_df["location.district.name"].apply(lambda x: DistrictMatch(x))

main_df["location.district.name"], main_df["location.district.ID"]=zip(*match)

assert main_df["location.district.ID"].isna().sum()==0


# Add missing districts

dates=main_df["dateRange"].unique()

date_dist=[]
for date in dates:
    date_dist.extend([(date, key,value ) for key, value in dist_map.items()])

dist_date_df=pd.DataFrame(date_dist, columns=["dateRange","location.district.name","location.district.ID"])


main_df=main_df.merge(dist_date_df, on=["dateRange","location.district.name","location.district.ID"], how="right")

main_df.groupby(by="dateRange")["location.district.name"].nunique()

# add missing cols and their corresponding values from the config file
master_col_vals=D["column_values"]
master_cols=list(master_col_vals.keys())
cols_add=set(master_cols) - set(main_df.columns)

for col in cols_add:
    main_df[col]=master_col_vals[col]

main_df.loc[main_df["location.ulb.name"]=="BBMP", "location.admin.hierarchy"]="ULB"
main_df.loc[main_df["location.ulb.name"]!="BBMP", "location.admin.hierarchy"]="REVENUE"

# fixing date vars
    
main_df["dateRange"]=main_df["dateRange"].str.split("_")

def date_time_set(x: list) -> tuple:
    start_date=x[0]
    end_date=x[1]
    start_date=datetime.date(day=int(x[0][:2]), month=int(x[0][2:4]), year=int(x[0][4:8])).isoformat()
    end_date=datetime.date(day=int(x[1][:2]), month=int(x[1][2:4]), year=int(x[1][4:8])).isoformat()

    return([start_date, end_date], end_date)

dates=main_df["dateRange"].apply(lambda x: date_time_set(x))

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

# adding calc variables
main_df["survey.houseIndex.calc"]=(main_df["survey.housesPositive"]/main_df["survey.housesVisited"]) * 100
main_df["survey.containerIndex.calc"]=(main_df["survey.containersPositive"]/main_df["survey.containersSearched"]) * 100
main_df["survey.breteauIndex.calc"]=(main_df["survey.containersPositive"]/main_df["survey.housesVisited"]) * 100

# rounding-up int cols
int_cols=['summary.noOfTaluks', 'summary.noOfPhcs','summary.noOfHouses', 'survey.housesVisited', 'survey.housesPositive','survey.containersSearched', 'survey.containersPositive', 'survey.containersReduced']

for col in int_cols:
    main_df[col]=main_df[col].round(0)


float_cols=['survey.houseIndex', 'survey.containerIndex', 'survey.breteauIndex', 'survey.houseIndex.calc', 'survey.containerIndex.calc', 'survey.breteauIndex.calc']

for col in float_cols:
    main_df[col]=main_df[col].round(2)

# creating uuids
main_df["metadata.recordID"]=[uuid.uuid4() for i in range(len(main_df))]

# filtering & ordering dataset
main_df=main_df[master_cols]

print(main_df.columns, len(main_df))

main_df.to_csv("ka-dengue-larval-survey.csv", index=False)

# The End!
