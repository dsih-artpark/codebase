import yaml
import pandas as pd
import re
import numpy as np
import uuid
import pandas as pd
from googlemaps import Client as GoogleMaps
import googlemaps
import gmaps
import subprocess


# FUNCTIONS

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


def CreateDummy(x: list):
    L=[re.sub(r"^\w\d", "", str(i).lstrip().rstrip()) for i in x]
    if "Borewell" in L:
        borewell=1
    else:
        borewell=0
    if "Rain water harvesting" in L:
        rainwater=1
    else:
        rainwater=0
    if "Private water tankers" in L:
        tank=1
    else:
        tank=0
    if "BWSSB/Municipal/Cauvery Water" in L:
        govt=1
    else:
        govt=0
    if "STP treated water" in L:
        stp=1
    else:
        stp=0
    return (borewell, rainwater, tank, govt, stp)

# Enter the path to your openssl encoded file
encrypted_file = "~/config.enc"

# Command to decrypt using OpenSSL - You will be prompted to enter the openssl password used for encryption
command = f"openssl aes-256-cbc -d -salt -in {encrypted_file}"

try:
    MyAPI=subprocess.check_output(command, shell=True, text=True).strip()
except subprocess.CalledProcessError as e:
    raise(e)

def geocode(full_address: str, MyAPI: str) -> tuple:
    """_summary_

    Args:
        full_address (str): concatenated address to include all relevant geographical fields

    Returns:
        tuple: lat, long
    """
    assert isinstance(MyAPI,str) and len(MyAPI)==39, "invalid input"
    gmaps = googlemaps.Client(key=MyAPI)

    if full_address!=np.nan or full_address!="":
        try:
            geocode_result = gmaps.geocode(full_address)
            if geocode_result:
                lat= geocode_result[0]['geometry']['location'] ['lat']
                long= geocode_result[0]['geometry']['location']['lng']
                return lat, long
            else:
                raise Exception("No result returned.")
        except Exception as e:
            print(f"Geocoding failed {e}")
    return None, None

#------------------------------------------------------------------

# STANDARDISATION FOR 2024
df=pd.read_csv("2024.csv")

with open("METADATA.yaml", "r") as f:
    D=yaml.safe_load(f)

# renaming preprocessed columns to their standardised names
preprocessed_col_map=D["tables"]["survey_2024.csv"]["column_mapping"]

df.columns=[col.lstrip().rstrip() for col in df.columns]
df.columns=[MapColumns(col, preprocessed_col_map) for col in df.columns]


# cleaning columns
df["location.geometry.pincode"]=df["location.geometry.pincode"].astype(str)
df[df["location.geometry.pincode"].str.len()!=6]


int_cols=["survey.numberOfHousingUnits", "survey.numberOfTankersPerMonth", "survey.capacityPerTanker", "survey.cost.tanker.present", "survey.cost.tanker.previousYear", "survey.cost.communityMonthlyWaterExpenses"]

# fixing dtypes
for col in int_cols:
    if df[col].dtype=="object":
        df[col]=df[col].apply(lambda x: NumvarStd(x))
        df[col]=df[col].astype(float)

for col in df.columns:
    if df[col].dtype=="object":
        df[col]=df[col].str.lstrip().str.rstrip().str.upper()


df["survey.source.all"]=df["survey.source.all"].str.split(",")

result=df["survey.source.all"].apply(lambda x: CreateDummy(x))

df["survey.source.borewell"], df["survey.source.rainwater"], df["survey.source.tanker"], df["survey.source.govt"], df["survey.source.STP"]= zip(*result)

# add std columns 
master_col_vals=D["tables"]["survey_2024.csv"]["column_values"]
master_cols=list(master_col_vals.keys())
cols_add=set(master_cols) - set(df.columns)

for col in cols_add:
    df[col]=master_col_vals[col]


# create recordID
df["metadata.recordID"]=[uuid.uuid4() for i in range(len(df))]

# geocode


# Apply function
df["location.geometry.latitude.imputed"], df["location.geometry.longitude.imputed"] = zip(*df["location.geometry.pincode"].apply(lambda x: geocode(x, MyAPI)))

# Filtering and ordering columns needed
df=df[master_cols]
df.to_csv("survey_2024.csv", index=False)

# ------------------------------------------------------------------

# STANDARDISATION FOR 2019
df=pd.read_csv("2019.csv")

with open("METADATA.yaml", "r") as f:
    D=yaml.safe_load(f)

# renaming preprocessed columns to their standardised names
preprocessed_col_map=D["tables"]["survey_2019"]["column_mapping"]

df.columns=[col.lstrip().rstrip() for col in df.columns]
df.columns=[MapColumns(col, preprocessed_col_map) for col in df.columns]

int_cols=["survey.numberOfHousingUnits", "survey.numberOfTankersPerMonth", "survey.capacityPerTanker", "survey.cost.tanker.present"]

# fixing dtypes
for col in int_cols:
    if df[col].dtype=="object":
        df[col]=df[col].apply(lambda x: NumvarStd(x))
        df[col]=df[col].astype(float)

for col in df.columns:
    if df[col].dtype=="object":
        df[col]=df[col].str.lstrip().str.rstrip().str.upper()

df["survey.source.all"]=df["survey.source.all"].str.split("\n")

result=df["survey.source.all"].apply(lambda x: CreateDummy(x))

df["survey.source.borewell"], df["survey.source.rainwater"], df["survey.source.tanker"], df["survey.source.govt"], df["survey.source.STP"]= zip(*result)

# add std columns 
master_col_vals=D["tables"]["2019.csv"]["column_values"]
master_cols=list(master_col_vals.keys())
cols_add=set(master_cols) - set(df.columns)

for col in cols_add:
    df[col]=master_col_vals[col]


# create recordID
df["metadata.recordID"]=[uuid.uuid4() for i in range(len(df))]


# Geocoding
df["survey.address"]=df["survey.address"].apply(lambda x: np.nan if not re.search(r"\D",str(x)) else x)

geocode_df=df[df["survey.address"].isna()==False]

geocode_df["survey.address"]=geocode_df["survey.address"]+","+"BENGALURU,KARNATAKA,INDIA"
geocode_df["location.geometry.latitude.imputed"], geocode_df["location.geometry.longitude.imputed"] = zip(*geocode_df["survey.address"].apply(lambda x: geocode(x, MyAPI)))

df=df[df["survey.address"].isna()]._append(geocode_df)

# Filtering and ordering columns needed
df=df[master_cols]
df.to_csv("survey_2019.csv", index=False)

