import os
import pandas as pd
import re
from epipipeline.preprocess import (
    clean_colname, map_column, extract_test_method_with_result)
from epipipeline.standardise.gis import *

from epipipeline.standardise import *
from epipipeline.standardise.dates import *
import yaml
import uuid
from dataio import download


def subdist_mapping(*, districtID: str, subdistName: str, df: pd.DataFrame, threshold: int = 65) -> tuple:
    """Standardises subdistrict/ulb names and codes (based on LGD), provided the standardised district ID

    Args:
        districtID (str): standarised district ID
        subdistName (str): raw subdistrict/ulb name
        df (pd.DataFrame): regions.csv as a dataframe
        threshold (int): cut-off for fuzzy matching, default set to 65

    Returns:
        tuple: (LGD subdistrict/ulb name, LGD subdistrict/ulb code or admin_0 if not matched)
    """
    # subdist
    if pd.isna(subdistName):
        return (pd.NA, "admin_0")

    subdistName = str(subdistName).title().strip()
    subdistName = re.sub(r'\(?\sU\)?$', "Urban",
                         subdistName, flags=re.IGNORECASE)
    subdistName = re.sub(r'\(?\sR\)?$', "Rural",
                         subdistName, flags=re.IGNORECASE)
    subdistricts = df[(df["parentID"] == districtID) & (df["regionID"].str.startswith("sub"))]["regionName"].to_list()
    match = process.extractOne(
        subdistName, subdistricts, score_cutoff=threshold)
    if match:
        subdistName = match[0]
        subdistCode = df[(df["parentID"] == districtID) & (
            df["regionName"] == subdistName)]["regionID"].values[0]
        return (subdistName, subdistCode)
    else:
        return (subdistName, "admin_0")  # returns original name if unmatched


def ulb_mapping(*, districtID: str, subdistName: str, df: pd.DataFrame, threshold: int = 60) -> tuple:
    """Standardises subdistrict/ulb names and codes (based on LGD), provided the standardised district ID

    Args:
        districtID (str): standarised district ID
        subdistName (str): raw subdistrict/ulb name
        df (pd.DataFrame): regions.csv as a dataframe
        threshold (int): cut-off for fuzzy matching, default set to 65

    Returns:
        tuple: (LGD subdistrict/ulb name, LGD subdistrict/ulb code or admin_0 if not matched)
    """
    # subdist
    if pd.isna(subdistName):
        return (pd.NA, "admin_0")

    subdistName = str(subdistName).title().strip()
    subdistName = re.sub(r'\(?\sU\)?$', "Urban",
                         subdistName, flags=re.IGNORECASE)
    subdistName = re.sub(r'\(?\sR\)?$', "Rural",
                         subdistName, flags=re.IGNORECASE)
    subdistricts = df[(df["parentID"] == districtID) & (df["regionID"].str.startswith("ulb"))]["regionName"].to_list()
    match = process.extractOne(
        subdistName, subdistricts, score_cutoff=threshold)
    if match:
        subdistName = match[0]
        subdistCode = df[(df["parentID"] == districtID) & (
            df["regionName"] == subdistName)]["regionID"].values[0]
        return (subdistName, subdistCode)
    else:
        return (subdistName, "admin_0")  # returns original name if unmatched

# set-up
with open("metadata.yaml") as file:
    D = yaml.safe_load(file)["tables"]["ka_ihip"]

COLUMNS = D["data_dictionary"]
STANDARD_MAPPER = D["admin"]["config"]["header_mapper"]
MIN_COLS = D["admin"]["config"]["min_cols"]
STR_VARS = D["admin"]["config"]["str_cols"]

regions=pd.read_csv("regions/regionids.csv")

main_df=pd.DataFrame()

for file in os.listdir():
    if file.startswith("file") and file.endswith(".csv"):
        df=pd.read_csv(file)
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

        # convert to datetime format
        for datevar in ['event.symptomOnsetDate', 'event.test.sampleCollectionDate', 'event.test.resultDate']:
            df[datevar]=pd.to_datetime(df[datevar])
            print(f"min date: {df[datevar].min()}, max date: {df[datevar].max()}")
            print(f"{df[datevar].dt.month.unique()}")

        # filter empty rows
        df.dropna(how="all", inplace=True, axis=0)

        main_df=main_df._append(df)


# add standardised cols
for col in COLUMNS.keys():
    if col not in main_df.columns:
        if "default_value" in COLUMNS[col].keys():
            main_df[col] = COLUMNS[col]["default_value"]
        else:
            main_df[col]=pd.NA

# # drop duplicates - log duplicates
main_df.drop_duplicates(inplace=True)

### STANDARDISATION ####
# Standardise Age
main_df["demographics.age"] = main_df["demographics.age"].apply(
    lambda x: standardise_age(age=x))

# Validate Age - 0 to 105
main_df["demographics.age"] = main_df["demographics.age"].apply(
    lambda x: validate_age(age=x))

# Bin Age
main_df["demographics.ageRange"] = pd.cut(main_df["demographics.age"].fillna(
    -999), bins=[0, 1, 6, 12, 18, 25, 45, 65, 105], include_lowest=False)
main_df.loc[main_df["demographics.age"].isna(
), "demographics.ageRange"] = pd.NA

#Standardise Gender - MALE, FEMALE, UNKNOWN
main_df["demographics.gender"] = main_df["demographics.gender"].apply(
    lambda x: standardise_gender(gender=x))

# Standardise Result variables - POSITIVE, NEGATIVE, UNKNOWN
main_df["event.test.test1.result"] = main_df["event.test.test1.result"].apply(
    lambda x: standardise_test_result(result=x))
main_df["event.test.test2.result"] = main_df["event.test.test2.result"].apply(
    lambda x: standardise_test_result(result=x))

# Generate test count - [0,1,2]
main_df["event.test.numberOfTests"] = main_df.apply(lambda x: generate_test_count(
    test1=x["event.test.test1.result"], test2=x["event.test.test2.result"]), axis=1)

# Standardise case variables
# OPD, IPD
main_df["case.opdOrIpd"] = main_df["case.opdOrIpd"].apply(
    lambda x: opd_ipd(s=x))

# Clean ID vars
ids = ['metadata.patientHealthID',
       'metadata.patientTransactionID', 'metadata.patientSpecimenID']

for id in ids:
    main_df[id] = main_df[id].str.replace(r"[^0-9\-]", "", regex=True)


# Fix date variables
datevars = ["event.symptomOnsetDate",
            "event.test.sampleCollectionDate", "event.test.resultDate"]

# Then, carry out year and date logical checks and fixes on symptom and sample date first

for datevar in datevars:
    df[datevar] = df[datevar].apply(lambda x: fix_year(Date=x, tagDate = "2024-07-16"))


result = main_df.apply(lambda x: fix_two_dates(
    earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"]), axis=1)
main_df["event.symptomOnsetDate"], main_df["event.test.sampleCollectionDate"] = zip(
    *result)

# Then, carry out year and date logical checks and fixes on symptom and sample date first
result = main_df.apply(lambda x: fix_two_dates(
    earlyDate=x["event.test.sampleCollectionDate"], lateDate=x["event.test.resultDate"]), axis=1)
main_df["event.test.sampleCollectionDate"], main_df["event.test.resultDate"] = zip(
    *result)

# One last time on symptom and sample date..for convergence..miracles do happen!
result = main_df.apply(lambda x: fix_two_dates(
    earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"]), axis=1)
main_df["event.symptomOnsetDate"], main_df["event.test.sampleCollectionDate"] = zip(
    *result)

for var in datevars:
    main_df[var].dt.month.unique()
    main_df[var].dt.year.unique()

# format dates to ISO format
for var in datevars:
    main_df[var] = main_df[var].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

# Setting primary date - symptom date > sample date > result date
main_df["metadata.primaryDate"] = main_df["event.symptomOnsetDate"].fillna(main_df["event.test.sampleCollectionDate"]).fillna(main_df["event.test.resultDate"])  # noqa: E501

# Clean string vars
for var in STR_VARS:
    if var in main_df.columns:
        main_df[var] = main_df[var].apply(lambda x: clean_strings(s=x))

# # Drop invalid address (BENGALURU (KARNATAKA) or BANGALORE (KARNATAKA)) # LOG
main_df.loc[(main_df["metadata.address"] == "Bangalore") | (main_df["metadata.address"] == "Bengaluru") | (
    main_df["metadata.address"] == "Bangalore Karnataka") | (main_df["metadata.address"] == "Bengaluru Karnataka") | (main_df["metadata.address"] == "Adl"), "metadata.address"] = pd.NA

# # Delete contact number from address
main_df["metadata.address"] = main_df["metadata.address"].str.replace(
    r"(PH)?\d{10}", "", regex=True)

# geomapping

# districts
res=main_df.apply(lambda x: dist_mapping(stateID="state_29", districtName=x["location.admin2.name"], df=regions), axis=1)
main_df["location.admin2.name"], main_df["location.admin2.ID"]=zip(*res)


# where ulb is provided, make subdistrict nan
main_df.loc[main_df["ulb"].isna()==False, "location.admin3.name"]=pd.NA

# subdists
res=main_df.apply(lambda x: subdist_mapping(districtID=x["location.admin2.ID"] , subdistName=x["location.admin3.name"], df=regions), axis=1)
main_df["location.admin3.name"], main_df["location.admin3.ID"]=zip(*res)

#ulb
res=main_df.apply(lambda x: ulb_mapping(districtID=x["location.admin2.ID"] , subdistName=x["ulb"], df=regions), axis=1)
main_df["ulb"], main_df["ulb_id"]=zip(*res)

main_df["ulb"].value_counts()

# replace admin3a with admin3
main_df.loc[main_df["ulb"].notna(), ["location.admin3.name", "location.admin3.ID"]] = main_df.loc[main_df["ulb"].notna(), ["ulb", "ulb_id"]].values

main_df.drop(columns=["ulb", "ulb_id"], inplace=True)
# village/ward
res=main_df.apply(lambda x: village_ward_mapping(subdistID=x["location.admin3.ID"], villageName=x["location.admin5.name"], df=regions), axis=1)
main_df["location.admin5.name"], main_df["location.admin5.ID"]= zip(*res)

# admin hierarchy
main_df["location.admin.hierarchy"] = main_df["location.admin3.ID"].apply(lambda x: "ULB" if str(x).startswith("ulb") else ("Revenue" if str(x).startswith("subdistrict") else "admin_0"))  # noqa: E501

# Creating recordID
main_df["metadata.recordID"] = [uuid.uuid4() for _ in range(len(main_df))]

wards_df=regions[regions["parentID"].str.startswith("zone_276600")]

def ward_mapping(wardname, df):
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


wards_df["regionName"]=wards_df["regionName"]+" Ward Number " + wards_df["regionID"].str.split("-").str.get(1)

bbmp=main_df[main_df["location.admin3.ID"]=="ulb_276600"]

main_df=main_df[main_df["location.admin3.ID"]!="ulb_276600"]

res=bbmp.apply(lambda x: ward_mapping(wardname=x["location.admin5.name"], df=wards_df), axis=1)

res

bbmp["location.admin5.name"], bbmp["location.admin5.ID"], bbmp["location.admin4.ID"]= zip(*res)

bbmp["location.admin4.name"]=bbmp["location.admin4.ID"].apply(lambda x: regions[regions["regionID"]==x]["regionName"].values[0] if x!="admin_0" else pd.NA)

main_df=main_df._append(bbmp)

L=["location.admin2.ID", "location.admin3.ID", "location.admin4.ID", "location.admin5.ID"]

import numpy as np
for vars in L:
    main_df[vars]=main_df[vars].replace("admin_0", np.nan)

main_df["location.admin.coarseness"]=main_df["location.admin5.ID"].fillna(main_df["location.admin4.ID"]).fillna(main_df["location.admin3.ID"]).fillna(main_df["location.admin2.ID"]).fillna(main_df["location.admin1.ID"]).str.split("_").str.get(0)

for vars in L:
    main_df[vars]=main_df[vars].fillna("admin_0")

main_df=main_df[COLUMNS.keys()]

main_df.to_csv("ihip_new_temp.csv", index=False)

df=pd.read_csv("data/EP0005DS0014-KA_Dengue_LL/ihip/ka-line-list-ihip.csv")

df=df._append(main_df)

df.columns

df.drop(columns=["metadata.name", "metadata.address","metadata.contact"], inplace=True)

df=df.drop_duplicates(subset= [col for col in df.columns if col!="metadata.recordID"])

df.rename(columns={"geography.coarseness":"geography.admin.coarseness"}, inplace=True)

df=df[['metadata.recordID', 'metadata.primaryDate', 'metadata.patientHealthID',
       'metadata.patientTransactionID', 'metadata.patientSpecimenID',
       'metadata.diseaseName', 'metadata.diseaseCode', 'demographics.age',
       'demographics.ageRange', 'demographics.gender', 'location.country.ID',
       'location.country.name', 'location.admin.hierarchy',
       'location.admin1.ID', 'location.admin1.name', 'location.admin2.ID',
       'location.admin2.name', 'location.admin3.ID', 'location.admin3.name',
       'location.admin4.ID', 'location.admin4.name', 'location.admin5.ID',
       'location.admin5.name', 'location.admin.coarseness','location.geometry.latitude.provided',
       'location.geometry.longitude.provided', 'event.symptomOnset',
       'event.symptomOnsetDate', 'event.test',
       'event.test.sampleCollectionDate', 'event.test.testingLab',
       'event.test.test1.code', 'event.test.test1.name',
       'event.test.test1.result', 'event.test.test2.code',
       'event.test.test2.name', 'event.test.test2.result',
       'event.test.resultDate', 'event.test.sampleType', 'case.opdOrIpd']]

df.to_csv("ka-line-list-ihip.csv", index=False)

df=pd.read_csv("ka-line-list-ihip.csv")