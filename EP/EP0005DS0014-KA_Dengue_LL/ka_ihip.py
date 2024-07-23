import os
import pandas as pd
import re
from epipipeline.preprocess import (clean_colname, map_column, extract_test_method_with_result)
from epipipeline.standardise.gis import (dist_mapping, subdist_ulb_mapping, village_ward_mapping, clean_lat_long)
from epipipeline.standardise import (validate_age, standardise_age, standardise_gender, standardise_test_result, clean_strings, opd_ipd)
from epipipeline.standardise.dates import (string_clean_dates, fix_two_dates, check_date_bounds)
import yaml
import uuid
from dataio import download
from fuzzywuzzy import process
import numpy as np
import datetime


## custom function

def bbmp_ward_mapping(wardname, df):
    if pd.isna(wardname):
        return (wardname, "admin_0", "admin_0")
    wards=df["regionName"].to_list()
    match = process.extractOne(wardname, wards, score_cutoff=60)
    if match:
        wardname = match[0]
        wardcode= df[(df["regionName"] == wardname)]["regionID"].values[0]
        zoneid=df[(df["regionName"] == wardname)]["parentID"].values[0]
        return (wardname, wardcode, zoneid)

    else:
        return (wardname, "admin_0", "admin_0")  

# set-up
with open("metadata.yaml") as file:
    D = yaml.safe_load(file)["tables"]["ka_ihip"]


COLUMNS = D["data_dictionary"]
STANDARD_MAPPER = D["admin"]["config"]["header_mapper"]
MIN_COLS = D["admin"]["config"]["min_cols"]
STR_VARS = D["admin"]["config"]["str_cols"]

# CHANGE AS INPUT
reportDate = pd.to_datetime("2024-07-21")
minimumDate = pd.to_datetime("2024-05-01")

regions=pd.read_csv("regions/regionids.csv")

df=pd.read_excel("Dengue Line Listing in Karnataka for GEO-COORDINATES - 21-07-2024.xlsx")

# string clean colnames - can be optimised by changing map_columns function to single-input
headers = [clean_colname(colname=col) for col in df.columns]
df.columns=headers

# get mapping of cols to standard cols
header_mapper = map_column(map_dict=STANDARD_MAPPER)

df.rename(columns=header_mapper, inplace=True)


# extract test results from test cols
if "test_method" and "result" in df.columns:
    tests = df.apply(lambda x: extract_test_method_with_result(
        test_method=x["test_method"], result=x["result"]), axis=1)
    df["event.test.test1.result"], df["event.test.test2.result"] = zip(
        *tests)

# check minimum cols present
min_cols_missing = set(MIN_COLS) - set(df.columns)
assert not min_cols_missing, f"File: {file} is missing minimum required columns {min_cols_missing}"

# Fix datevars

datevars=['event.symptomOnsetDate', 'event.test.sampleCollectionDate', 'event.test.resultDate']
for datevar in datevars:
    df[datevar] = df[datevar].apply(lambda x: string_clean_dates(Date = x))
    # cannot apply fix_year_for_ll function from epipipeline, as year is only 2024
    if len(df[df[datevar].dt.year!=2024])>0:
        df[datevar].apply(lambda x: datetime.datetime(day = x.day, month = x.month, year=2024) if x.year!=2024 else x)

result = df.apply(lambda x: fix_two_dates(
    earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"], tagDate = reportDate, minDate = minimumDate), axis=1)
df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(
    *result)

# Then, carry out year and date logical checks and fixes on symptom and sample date first
result = df.apply(lambda x: fix_two_dates(
    earlyDate=x["event.test.sampleCollectionDate"], lateDate=x["event.test.resultDate"], tagDate=reportDate, minDate = minimumDate), axis=1)
df["event.test.sampleCollectionDate"], df["event.test.resultDate"] = zip(
    *result)

# One last time on symptom and sample date..for convergence..miracles do happen!
result = df.apply(lambda x: fix_two_dates(
    earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"], tagDate=reportDate, minDate=minimumDate), axis=1)
df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(
    *result)

# Check dates to reportDate and convert to ISO format
for datevar in datevars:
    df[datevar] = df[datevar].apply(lambda x: check_date_bounds(Date=x, minDate=minimumDate, tagDate=reportDate))
    print(f"{datevar}: min = {df[datevar].min()}, max = {df[datevar].max()}")
    df[datevar] = df[datevar].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

# Setting primary date - symptom date > sample date > result date
df["metadata.primaryDate"] = df["event.symptomOnsetDate"].fillna(df["event.test.sampleCollectionDate"]).fillna(df["event.test.resultDate"])  # noqa: E501

# Standardise Age
df["demographics.age"] = df["demographics.age"].apply(lambda x: standardise_age(age=x))

# Validate Age - 0 to 105
df["demographics.age"] = df["demographics.age"].apply(lambda x: validate_age(age=x))

# Bin Age
df["demographics.ageRange"] = pd.cut(df["demographics.age"].fillna(-999), bins=[0, 1, 6, 12, 18, 25, 45, 65, 105], include_lowest=False)
df.loc[df["demographics.age"].isna(), "demographics.ageRange"] = pd.NA

#Standardise Gender - MALE, FEMALE, UNKNOWN
df["demographics.gender"] = df["demographics.gender"].apply(lambda x: standardise_gender(gender=x))

# Standardise Result variables - POSITIVE, NEGATIVE, UNKNOWN
df["event.test.test1.result"] = df["event.test.test1.result"].apply(lambda x: standardise_test_result(result=x))
df["event.test.test2.result"] = df["event.test.test2.result"].apply(lambda x: standardise_test_result(result=x))

# Standardise case variables
# OPD, IPD
df["case.opdOrIpd"] = df["case.opdOrIpd"].apply(lambda x: opd_ipd(s=x))

# Clean ID vars
ids = ['metadata.patientHealthID','metadata.patientTransactionID', 'metadata.patientSpecimenID']

for id in ids:
    df[id] = df[id].str.replace(r"[^0-9\-]", "", regex=True)

# Clean string vars
for var in STR_VARS:
    if var in df.columns:
        df[var] = df[var].apply(lambda x: clean_strings(s=x))

# districts
res=df.apply(lambda x: dist_mapping(stateID="state_29", districtName=x["location.admin2.name"], df=regions), axis=1)
df["location.admin2.name"], df["location.admin2.ID"]=zip(*res)

# where BBMP is mentioned in the remarks, change ulb to BBMP
if "remark" in df.columns:
    df.loc[df["remark"].str.contains("BBMP", re.IGNORECASE)==True, "ulb"]="BBMP"

# where ulb is provided, make subdistrict nan before mapping subdistricts
df.loc[df["ulb"].isna()==False, "location.admin3.name"]=pd.NA

# subdists
res=df.apply(lambda x: subdist_ulb_mapping(districtID=x["location.admin2.ID"] , subdistName=x["location.admin3.name"], df=regions, childType="subdistrict"), axis=1)
df["location.admin3.name"], df["location.admin3.ID"]=zip(*res)

#ulb
res=df.apply(lambda x: subdist_ulb_mapping(districtID=x["location.admin2.ID"] , subdistName=x["ulb"], df=regions, childType="ulb"), axis=1)
df["ulb"], df["ulb_id"]=zip(*res)

# replace admin3a with admin3
df.loc[df["ulb"].notna(), ["location.admin3.name", "location.admin3.ID"]] = df.loc[df["ulb"].notna(), ["ulb", "ulb_id"]].values

df.drop(columns=["ulb", "ulb_id"], inplace=True)

# village/ward
res=df.apply(lambda x: village_ward_mapping(subdistID=x["location.admin3.ID"], villageName=x["location.admin5.name"], df=regions), axis=1)
df["location.admin5.name"], df["location.admin5.ID"]= zip(*res)

# tempfix - bbmp_wards mapping
wards_df=regions[regions["parentID"].str.startswith("zone_276600")]

bbmp=df[df["location.admin3.ID"]=="ulb_276600"]

df=df[df["location.admin3.ID"]!="ulb_276600"]

res=bbmp.apply(lambda x: bbmp_ward_mapping(wardname=x["location.admin5.name"], df=wards_df), axis=1)

bbmp["location.admin5.name"], bbmp["location.admin5.ID"], bbmp["location.admin4.ID"]= zip(*res)

bbmp["location.admin4.name"]=bbmp["location.admin4.ID"].apply(lambda x: regions[regions["regionID"]==x]["regionName"].values[0] if x!="admin_0" else pd.NA)

df=df._append(bbmp)

# Generate admin coarseness
L=["location.admin2.ID", "location.admin3.ID", "location.admin4.ID", "location.admin5.ID"]

for vars in L:
    df[vars]=df[vars].replace("admin_0", np.nan)

df["location.admin.coarseness"]=df["location.admin5.ID"].fillna(df["location.admin4.ID"]).fillna(df["location.admin3.ID"]).fillna(df["location.admin2.ID"]).fillna(df["location.admin1.ID"]).str.split("_").str.get(0)

for vars in L:
    df[vars]=df[vars].fillna("admin_0")

# Clean lat, long
res=df.apply(lambda x: clean_lat_long(lat = x["location.geometry.latitude.provided"], long = x["location.geometry.longitude.provided"]), axis=1)
df["location.geometry.latitude.provided"], df["location.geometry.longitude.provided"] = zip(*res)

# filter empty rows
df=df.dropna(how="all", axis=0)
df=df.dropna(how="all", axis=1)

# add standardised cols
for col in COLUMNS.keys():
    if col not in df.columns:
        if "default_value" in COLUMNS[col].keys():
            df[col] = COLUMNS[col]["default_value"]
        else:
            df[col]=pd.NA

# drop duplicates - log duplicates
df=df.drop_duplicates()

# generate recordID
df["metadata.recordID"]=[uuid.uuid4() for _ in range(len(df))]

# filter/order vars
df=df[COLUMNS.keys()]

# export current file
df.to_csv("ihip_new_temp.csv", index=False)

# append to main file
main_df=pd.read_csv("data/EP0005DS0014-KA_Dengue_LL/ihip/ka-line-list-ihip.csv")
df=df._append(main_df)

# drop duplicates
df=df.drop_duplicates(subset= [col for col in df.columns if col!="metadata.recordID"])

# export for manual checks - admin backward mapping
df.to_csv("ka-line-list-ihip.csv", index=False)
