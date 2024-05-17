import pandas as pd
import os
import re
import yaml
import numpy as np
from functions import *

# 0 - SETTING GLOBAL VARS FROM METADATA.YAML
with open("metadata.yaml") as f:
    D=yaml.safe_load(f)
    
COLUMN_MAP=D["column_mapping"]["historical"]
COLUMN_VALUES=D["column_values"]
COLUMN_MASTER=list(COLUMN_VALUES.keys())

# TO CHANGE - use function with year as parameter instead of input field below
CURRENT_YEAR=int(input("Enter the year of the file (integer)"))

# 1 - PREPROCESSING
main_df=pd.DataFrame()

# TO ADD: Import district-wise raw files from AWS S3

# Preprocess each file separately before appending to a single dataframe
for file in os.listdir(f"./{CURRENT_YEAR}"):
    if file.endswith(".csv"):
        df=pd.read_csv(f"./{CURRENT_YEAR}/{file}")

        # adding district name from filename
        df["district"]=file.split(".")[0]

        # drop extra empty columns
        for col in df.columns:
            if re.search(r"unnamed.", col, re.IGNORECASE):
                df.drop(columns=[col], inplace=True)

         # standardise test columns
        if "test_method" in df.columns and "result" in df.columns:
            tests=df.apply(lambda x: extract_test_method_with_result(x["test_method"], x["result"]), axis=1)
            df["ns1"], df["igm"]=zip(*tests)
        elif "test_method" in df.columns:
            tests=df.apply(lambda x: extract_test_method_without_result(x["test_method"]), axis=1)
            df["ns1"], df["igm"]=zip(*tests)

        # map to new col names
        df.columns=[map_columns(col, COLUMN_MAP) for col in df.columns]    

        # merging name & address
        if "name" in df.columns and "address" in df.columns:
            df["metadata.nameAddress"]=df["name"] + " , "+ df["address"]  
            df.drop(columns=["name","address"], inplace=True)  
        elif "name" in df.columns:
            df.rename(columns={"name":"metadata.nameAddress"}, inplace=True)

        # nullifying patient nameAddress that do not contain a single alphabet
        df["metadata.nameAddress"]=df["metadata.nameAddress"].apply(lambda x: x if re.search(r"[A-Za-z]", str(x).strip()) else pd.NA)
        
        # # dropping null patient nameAddress
        df.dropna(subset=["metadata.nameAddress"], inplace=True, how="all")   

        # # extracting mobile numbers from, address and removing mobile number from address
        if "metadata.contact" not in df.columns:
            result=df["metadata.nameAddress"].apply(lambda x: extract_contact(x))
            df["metadata.nameAddress"], df["metadata.contact"]=zip(*result)
        
        # #  separating age and gender
        if "agegender" in df.columns:
            demographics=df["agegender"].apply(lambda x: extract_age_gender(x))
            df["demographics.age"], df["demographics.gender"] = zip(*demographics)

        # dropping extraneous rows & columns
        df.dropna(how="all", axis=0, inplace=True)
        df.dropna(how="all", axis=1,inplace=True)

        # skip if file is empty
        if len(df)==0:
            continue

        # append district to master
        main_df=main_df._append(df)


# adding standard list of columns from metadata that are not present in the dataset
for col in COLUMN_MASTER:
    if col not in main_df.columns:
        main_df[col]=COLUMN_VALUES[col]["value"]

# filtering and ordering dataframe cols, retaining only those in metadata.yaml
main_df=main_df[COLUMN_MASTER]

assert main_df["location.admin2.name"].nunique()==31, "District(s) missing"

# Locally export preprocessed file for standardisation instead of uploading to AWS S3
## Note: not generating patient and metadata record ID at this stage:
## 1) patient id requires standardised age, gender and clean name address
## 2) record id requires de-duplication which only be done after standardising age, gender, dates, etc.

main_df.to_csv(f"preprocessed_{CURRENT_YEAR}.csv", index=False)

