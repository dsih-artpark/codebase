## ET0004DS0001-BLR_PRISM_App_Larval_Survey
import os
import json
import yaml
import pandas as pd
from epipipeline.preprocess import (clean_colname, map_column)
from epipipeline.standardise import (clean_strings)
import re
import logging
from dataio.download import download_dataset_v2
import fuzzywuzzy as fpd
from fuzzywuzzy import process
from epipipeline import get_regionIDs
import uuid

#------SET-UP------#
# Basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Import yaml
with open('metadata.yaml') as yml:
    D=yaml.safe_load(yml)
    D=D["tables"]["prism-app-v1"]
    yml.close()

data_dict = D["data_dictionary"]
str_vars = D["config"]["str_vars"]
min_cols = D["config"]["required_cols"]
standard_mapper = D["config"]["column_mapper"]
header_mapper=map_column(map_dict=standard_mapper)

# setting up data structures for geomapping
download_dataset_v2(dsid="GS0015DS0034", suffixes=".csv")
region_ids, region_ids_dict = get_regionIDs()

region_ids_dict["zone_276600-1"].get("regionName")

# creating a dict where key = ward name (only bbmp) and value is a dict with regionID and parentID key-value pairs
bbmp_wards=region_ids[region_ids["parentID"].str.startswith("zone_276600")]

wards_map=dict()
for idx, val in bbmp_wards.iterrows():
    if val["regionName"] not in wards_map.keys():
        wards_map[val["regionName"]] = dict()
        wards_map[val["regionName"]]["regionID"] = val["regionID"]
        wards_map[val["regionName"]]["parentID"] = val["parentID"]

# function to fuzzymatch ward names and retrieve ward ID, zone ID, std ward name

def get_ward_zone(x: str, D: dict, L: list):

    if pd.isna(x):
        return ("admin_0", x, "admin_0")
    
    match, score = process.extractOne(x, L)
    if score>65:
        ward_name = match
        ward_ID = D[match]["regionID"]
        zone_ID = D[match]["parentID"]
    else:
        ward_name = x
        ward_ID = "admin_0"
        zone_ID = "admin_0"
    return (ward_ID, ward_name, zone_ID)

def get_zone_name(x, D):
    if x=="admin_0":
        return "admin_0"
    else:
        try:
            return D[x]["regionName"]
        except KeyError:
            return "admin_0"


#-----STANDARDISE-----#
# Import raw file
with open('data_update1_part3.json') as f:
    d = json.load(f)
    f.close()

df = pd.DataFrame(d["data"])


def standardise_prism_app(*, df: pd.DataFrame, 
                          data_dict: dict, 
                          header_mapper: dict, 
                          min_cols: list,
                          str_vars: list,
                          wards_map: dict,
                          region_ids_dict: dict):

    # drop cols with all na values

    df = df.replace({"None": pd.NA, None: pd.NA})
    df=df.dropna(how="all")

    # clean headers
    headers = [clean_colname(colname=col) for col in df.columns]
    df.columns = headers

    # extract lat,long to separate vars
    if "latitude_and_longitude" in df.columns:
        df["latitude"] = df["latitude_and_longitude"].str.split(",").str[0]
        df["longitude"] = df["latitude_and_longitude"].str.split(",").str[1]
        df.drop(columns="latitude_and_longitude", inplace=True)
    else:
        logging.info(f"File {f} does not have var latitude and longitude")

    # map colnames to standardised names
    df=df.rename(columns=header_mapper)

    # check min cols
    if not set(min_cols).issubset(set(df.columns)):
        raise ValueError(f"File {f} is missing cols {set(min_cols) - set(df.columns)}")
    
    # convert to datetime
    df["metadata.primaryDateTime"] = pd.to_datetime(df["metadata.primaryDateTime"], format = "%Y-%m-%d %H:%M:%S").dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    # clean str vars
    for col in str_vars:
        df[col] = df[col].apply(lambda x: clean_strings(s=x))

    # add std cols and std values
    for col in data_dict.keys():
        if col not in df.columns:
            if "default_value" in data_dict[col].keys():
                df[col]=data_dict[col]["default_value"]
                logging.info(f"Adding default value for cols {col}")
            else:
                logging.info(f"Adding col {col} without default value")
                df[col]=pd.NA

    # get ward name, ID, zone ID then retrieve zone name
    res=df["location.admin5.name"].apply(lambda x: get_ward_zone(x, wards_map, wards_map.keys()))
    df["location.admin5.ID"], df["location.admin5.name"], df["location.admin4.ID"] = zip(*res)
    df["location.admin4.name"]=df["location.admin4.ID"].apply(lambda x: get_zone_name(x, region_ids_dict))

    #drop duplicates, assign uuid4
    df.drop_duplicates(inplace=True)
    df["metadata.recordID"] = [uuid.uuid4() for _ in range(len(df))]

    # filter and sort cols
    df=df[list(data_dict.keys())]

    return df
