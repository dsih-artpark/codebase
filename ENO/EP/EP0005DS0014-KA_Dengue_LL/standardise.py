import pandas as pd
import re
import yaml
import numpy as np
import datetime
import boto3
from fuzzywuzzy import process
import uuid
from functions import *

# 0 - IMPORTING GLOBAL VARS FROM METADATA.YAML

# CHANGE - Import from Github
with open("metadata.yaml") as f:
    D=yaml.safe_load(f)

STR_VARS=D["config"]["str_cols"]
THRESHOLDS=D["config"]["thresholds"]
PII_FIELDS=D["config"]["pii"]

CURRENT_YEAR=int(input("Enter the year of the file (integer)"))

# get regions.csv
s3 = boto3.client('s3')
bucket_name = 'dsih-artpark-03-standardised-data'
file_path = 'GS0015DS0034-LGD_Region_IDs_and_Names/regionids.csv'

try:
    s3.download_file(bucket_name, file_path)
except Exception as e:
    print(f'File did not download: {e}')

regions=pd.read_csv("regions.csv")


# 1 - STANDARDISATION
df=pd.read_csv(f"preprocessed_{CURRENT_YEAR}.csv")

# Standardise Age
df["demographics.age"]=df["demographics.age"].apply(lambda x: standardise_age(x))

# Validate Age - 0 to 105
df["demographics.age"]=df["demographics.age"].apply(lambda x: validate_age(x))

# Bin Age
df["demographics.ageRange"]=pd.cut(df["demographics.age"].fillna(-999), bins=[0, 1, 6, 12, 18, 25, 45, 65, 105], include_lowest=False)
df.loc[df["demographics.age"].isna(), "demographics.ageRange"]=pd.NA

# Standardise Gender - MALE, FEMALE, UNKNOWN
df["demographics.gender"]=df["demographics.gender"].apply(lambda x: standardise_gender(x))

### Standardise Result variables - POSITIVE, NEGATIVE, UNKNOWN
df["event.test.test1.result"]=df["event.test.test1.result"].apply(lambda x: standardise_test_result(x))
df["event.test.test2.result"]=df["event.test.test2.result"].apply(lambda x: standardise_test_result(x))

## Generate test count - [0,1,2]
df["event.test.numberOfTests"]=df.apply(lambda x: generate_test_count(x["event.test.test1.result"], x["event.test.test2.result"]), axis=1)

# Standardise case variables 
## OPD, IPD
df["case.opdOrIpd'"]=df["case.opdOrIpd'"].apply(lambda x: opd_ipd(x))

## PUBLIC, PRIVATE
df["case.publicOrPrivate"]=df["case.publicOrPrivate"].apply(lambda x: public_private(x))

## ACTIVE, PASSIVE
df["case.surveillance"]=df["case.surveillance"].apply(lambda x: active_passive(x))

# URBAN, RURAL
df["case.urbanOrRural"]=df["case.urbanOrRural"].apply(lambda x: rural_urban(x))

# Fix date variables
datevars=["event.symptomOnsetDate", "event.test.sampleCollectionDate","event.test.resultDate"]

# Fix symptom date where number of days is entered instead of date
new_dates=df.apply(lambda x: fix_symptom_date(x["event.symptomOnsetDate"], x["event.test.resultDate"]), axis=1)
df["event.symptomOnsetDate"], df["event.test.resultDate"] = zip(*new_dates)

# Then, string clean dates
for var in datevars:
    df[var]=df[var].apply(lambda x: string_clean_dates(x))

# Then, carry out year and date logical checks and fixes on symptom and sample date first
result=df.apply(lambda x: fix_two_dates(x["event.symptomOnsetDate"], x["event.test.sampleCollectionDate"], CURRENT_YEAR), axis=1)
df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)

# Then, carry out year and date logical checks and fixes on symptom and sample date first
result=df.apply(lambda x: fix_two_dates(x["event.test.sampleCollectionDate"], x["event.test.resultDate"], CURRENT_YEAR), axis=1)
df["event.test.sampleCollectionDate"], df["event.test.resultDate"] = zip(*result)

# One last time on symptom and sample date..for convergence..miracles do happen! 
result=df.apply(lambda x: fix_two_dates(x["event.symptomOnsetDate"], x["event.test.sampleCollectionDate"], CURRENT_YEAR), axis=1)
df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)

# Setting primary date - symptom date > sample date > result date
df["metadata.primaryDate"]=df["event.symptomOnsetDate"].fillna(df["event.test.sampleCollectionDate"]).fillna(df["event.test.resultDate"])  # noqa: E501

# Clean string vars
for var in STR_VARS:
    if var in df.columns:
        df[var]=df[var].apply(lambda x: clean_strings(x))

# Geo-mapping
## Note: can be optimised to improve geo-mapping
# Move BBMP from district to subdistrict/ulb field
df.loc[df["location.admin2.name"]=="BBMP", "location.admin3.name"]="BBMP"

# Map district name to standardised LGD name and code
dists=df.apply(lambda x: dist_mapping(x["location.admin1.ID"], x["location.admin2.name"], regions, 
THRESHOLDS["district"]), axis=1)
df["location.admin2.name"], df["location.admin2.ID"]=zip(*dists)

assert len(df[df["location.admin2.ID"]=="admin_0"])==0, "District(s) missing"

# Map subdistrict/ulb name to standardised LGD name and code
subdist=df.apply(lambda x: subdist_ulb_mapping(x["location.admin2.ID"], x["location.admin3.name"], regions, 
THRESHOLDS["subdistrict"]), axis=1)
df["location.admin3.name"], df["location.admin3.ID"]=zip(*subdist)

# Map village/ward name to standardised LGD name and code
villages=df.apply(lambda x: village_ward_mapping(x["location.admin3.ID"], x["location.admin5.name"], regions, THRESHOLDS["village"] ), axis=1)
df["location.admin5.name"], df["location.admin5.ID"]=zip(*villages)

# Extract admin hierarchy from admin3.ID - ULB, REVENUE, admin_0 (if missing ulb/subdistrict LGD code)
df["location.admin.hierarchy"]=df["location.admin3.ID"].apply(lambda x: "ULB" if x.startswith("ulb") else ("REVENUE" if x.startswith("subdistrict") else "admin_0"))

# Drop duplicates across all vars after standardisation
df.drop_duplicates(inplace=True)

# Generate recordID after standardisation and de-duplication
df["metadata.recordID"]=[uuid.uuid4() for i in range(len(df))]

# Generate patient ID by grouping by nameAddress, age and gender
df["metadata.patientID"]=df.groupby(['metadata.nameAddress', 'demographics.age', 'demographics.gender'])["metadata.patientID"].transform(lambda x: uuid.uuid4())

# CHANGE - Push to AWS - To check date format when pushing to AWS directly
if not os.path.exists("./preprocessed"):
    os.makedirs("./preprocessed")

df.to_csv(f"preprocessed/{CURRENT_YEAR}.csv", index=False, date_format="%Y-%m-%d")

# Drop PII fields
std=df.drop(columns=PII_FIELDS)

# CHANGE - Push to AWS - To check date format when pushing to AWS directly
if not os.path.exists("./standardised"):
    os.makedirs("./standardised")

std.to_csv(f"standardised/{CURRENT_YEAR}.csv", index=False, date_format="%Y-%m-%d")