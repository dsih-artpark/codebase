import pandas as pd
import os
import re
import yaml
from fuzzywuzzy import process
import numpy as np


# Functions:
def map_columns(colname:str, map_dict: dict) -> str: 
    """_summary_

    Args:
        colname (str): Current column in DataFrame
        map (dict): Dictionary mapping of preprocessed col names to standardised col names

    Returns:
        str: Standardised column name
    """
    colname=re.sub(r"[^\w\s]","", colname.lower().lstrip().rstrip())
    colname= re.sub(r"\s\s"," ", colname.lower().lstrip().rstrip())
    colname=re.sub(r"\s","_", colname)

    for key, values in map_dict.items():
        if colname in values:
            return key
    return colname


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
main_df.dropna(subset=["location.village.name", "survey.housesVisited"], inplace=True)
main_df=main_df[main_df["location.district.name"]!="Total"]


# GEOMAPPING
regions=pd.read_csv("regionids.csv")

# some manual cleaning - dist & subdist mapping 
main_df["location.district.ID"]="district_539"
main_df["location.subdistrict.name"]=main_df["location.subdistrict.name"].str.replace("C.R.Patna","CHANNARAYAPATNA")
main_df["location.subdistrict.name"]=main_df["location.subdistrict.name"].str.replace("C.R.PATNA","CHANNARAYAPATNA")
main_df["location.subdistrict.name"]=main_df["location.subdistrict.name"].str.replace("H N PURA","HOLE NARSIPUR")
main_df["location.subdistrict.name"]=main_df["location.subdistrict.name"].str.replace("S K PURA","SAKLESHPUR")

subdist=regions[regions["parentID"]=="district_539"]

def mapsubdist(subdistname, df):
    matches=df["regionName"].to_list()

    match=process.extractOne(subdistname, matches, score_cutoff=65)

    if match:
        return (match[0], df[df["regionName"]==match[0]]["regionID"].values[0])
    else:
        return subdistname, np.nan

res=main_df["location.subdistrict.name"].apply(lambda x: mapsubdist(x, subdist))
main_df["location.subdistrict.name"], main_df["location.subdistrict.ID"]=zip(*res)

villages=regions[regions["parentID"].isin(main_df["location.subdistrict.ID"].unique())]

villages[(villages["parentID"]=="subdistrict_5557") & (villages["regionName"]=="BEDACHAVALLI")]


def village_map(villageName, subdistID, df):
    matches=df[df["parentID"]==subdistID]["regionName"].to_list()
    match=process.extractOne(villageName, matches, score_cutoff=95)
    if match:
        return (match[0], df[(df["parentID"]==subdistID) & (df["regionName"]==match[0])]["regionID"].values[0], subdistID)
    else:
        match=process.extractBests(villageName, df["regionName"].to_list(), score_cutoff=95)
        if match:
            if len(match)==1:
                return (match[0][0], df[df["regionName"]==match[0][0]]["regionID"].values[0], df[df["regionName"]==match[0][0]]["parentID"].values[0])
    return (villageName, np.nan, subdistID)


res=main_df.apply(lambda x: village_map(x["location.village.name"], x["location.subdistrict.ID"], villages), axis=1)

main_df["location.village.name"].isna().sum()
main_df["location.village.name"]
village_map("HUVINAHALLI", "subdistrict_5553", villages)
main_df[["location.village.name", "location.subdistrict.ID"]]

res=main_df.apply(lambda x: village_map(x["location.village.name"], x["location.village.ID"], x["location.subdistrict.ID"],  D), axis=1)

main_df["location.village.name"], main_df["location.village.ID"], main_df["location.subdistrict.ID"] = zip(*res)

# update subdist name


def mapsubdistID(subdistID, df):
    matches=df["regionID"].to_list()
    match=process.extractOne(subdistID, matches, score_cutoff=65)
    if match:
        return (match[0], df[df["regionID"]==match[0]]["regionName"].values[0])
    else:
        return subdistID, np.nan
    
res=main_df["location.subdistrict.ID"].apply(lambda x: mapsubdistID(x, subdist))
main_df["location.subdistrict.ID"], main_df["location.subdistrict.name"]= zip(*res)

main_df.loc[main_df["location.village.ID"].isna()==False, "location.admin.hierarchy"]="REVENUE"

for col in main_df.columns:
    if col.startswith("location"):
        if main_df[col].isna().sum()>0:
            print(col, main_df[col].isna().sum())

main_df["location.ward.ID"]=main_df["location.ward.ID"].fillna("ward_-1")
main_df["location.village.ID"]=main_df["location.village.ID"].fillna("village_-1")

main_df["location.village.name"]=main_df["location.village.name"].str.upper().str.lstrip().str.rstrip()

# checking that all subdistricts are included in every reporting period

df.groupby(by="metadata.report")
master=(regions[regions["parentID"]=="district_539"].regionID.unique())
len(main_df["location.subdistrict.ID"].unique())==len(master)

main_df.groupby(by="metadata.reportPeriod")["location.subdistrict.ID"].nunique()

# Fixing dates
main_df["metadata.reportPeriod"]=main_df["metadata.reportPeriod"].str.replace(".","-")

main_df["metadata.reportPeriod"]=main_df["metadata.reportPeriod"].str.split("to")

main_df["metadata.reportPeriod"].unique()

def date_time_set(x: list) -> tuple:
    import datetime
    start_date=x[0].lstrip().rstrip()
    end_date=x[1].lstrip().rstrip()
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

main_df.to_csv("source-reduction-hassan.csv", index=False)

df=pd.read_csv("source-reduction-hassan.csv")

len(df[df["survey.houseIndex"]!=df["survey.houseIndex.calc"]])/len(df) *100

len(df[df["survey.breteauIndex"]!=df["survey.breteauIndex.calc"]])/ len(df) *100

len(df[df["survey.containerIndex"]!=df["survey.containerIndex.calc"]])/ len(df) *100

len(df[df["location.village.ID"]=="village_-1"])/len(df) * 100


pd.DataFrame(df.groupby(by="metadata.primaryDate")[['survey.housesVisited', 'survey.housesPositive', 'survey.containersSearched', 'survey.containersPositive', 'survey.containersReduced']].sum()).reset_index().to_csv("summaries.csv", index=False)
