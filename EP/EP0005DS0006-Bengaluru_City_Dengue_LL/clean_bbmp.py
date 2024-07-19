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

# set-up
with open("metadata.yaml") as file:
    D = yaml.safe_load(file)["tables"]["ka_ihip"]

COLUMNS = D["data_dictionary"]
STANDARD_MAPPER = D["admin"]["config"]["header_mapper"]
MIN_COLS = D["admin"]["config"]["min_cols"]
STR_VARS = D["admin"]["config"]["str_cols"]

main_df = pd.DataFrame()

# PREPROCESSING
for file in os.listdir():
    if file.endswith(".xlsx"):
        # path = os.path.join("raw", file)

        df = pd.read_excel(file)

        print(f"pre: {file}, {len(df)}")

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


        # filter data to BBMP - log here
        df = df[df["location.ulb.name"].str.contains(
            r"BBMP", case=False, na=False)]


        # convert to datetime format
        for datevar in ['event.symptomOnsetDate', 'event.test.sampleCollectionDate', 'event.test.resultDate']:
            df[datevar] = pd.to_datetime(df[datevar], format="mixed", dayfirst=True)


        # filter empty rows
        df.dropna(how="all", inplace=True, axis=0)

        # add standardised cols
        for col in COLUMNS.keys():
            if col not in df.columns:
                if "default_value" in COLUMNS[col].keys():
                    df[col] = COLUMNS[col]["default_value"]


        main_df = pd.concat([main_df, df], ignore_index=True)


# # drop duplicates - log duplicates
main_df.drop_duplicates(inplace=True)


# main_df.to_csv("preprocessed-09-06-2024 to 26-06-2024.csv", index=False)

# main_df = pd.read_csv("preprocessed-09-06-2024 to 26-06-2024.csv")

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

# Extract Lab ID
main_df['event.test.testingLabID'] = main_df["metadata.patientSpecimenID"].str.split(
    "-").str.get(0)

# Fix date variables
datevars = ["event.symptomOnsetDate",
            "event.test.sampleCollectionDate", "event.test.resultDate"]

for var in datevars:
    main_df[var] = pd.to_datetime(main_df[var], dayfirst=True)

# Then, carry out year and date logical checks and fixes on symptom and sample date first
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


# Geocode address
my_api = get_api_key()

add = geocode(addresses=main_df['metadata.address'],
              API_key=my_api, batch_size=1000)

main_df['location.geometry.latitude'], main_df['location.geometry.longitude'] = zip(
    *add)

# Check bounds of address
regionID = COLUMNS["location.admin3.a.ID"]["default_value"]
# Download shapefile
download.download_dataset_v2(dsid="GS0012DS0051", contains_all=regionID)

res = check_bounds(lat=main_df['location.geometry.latitude'],
                   long=main_df['location.geometry.longitude'], regionID=regionID)

main_df["location.geometry.latitude"], main_df["location.geometry.longitude"] = zip(
    *res)

main_df.loc[main_df["location.geometry.latitude"].isna(
) == False, "location.geometry.coarseness"] = "Patient Address"

# Merge with lab geocode - download dataset, merge with lat, long (before adding std codes)
# - if lab not in file, geocode lab

labs = pd.read_csv("bbmp-testing-labs.csv")

L = set(labs['event.test.testingLab'])
L2 = set(main_df["event.test.testingLab"])

to_add = list(L2 - L)

for lab in to_add:
    res = geocode(addresses=lab, API_key=my_api)
    lat, long = res
    row=len(labs)
    labs.loc[row, "event.test.testingLab"] = lab
    labs.loc[row, "event.test.testingLab.latitude"] = lat
    labs.loc[row, "event.test.testingLab.longitude"] = long

main_df = main_df.merge(labs, on="event.test.testingLab", how="left")

main_df.drop(columns=["event.test.testingLab.latitude_x",
             "event.test.testingLab.longitude_x"], inplace=True)

main_df.rename(columns={
    "event.test.testingLab.latitude_y": "event.test.testingLab.latitude",
    "event.test.testingLab.longitude_y": "event.test.testingLab.longitude"}, inplace=True)

labs.to_csv("bbmp-testing-labs.csv", index=False)

# fillna for lat, long of patient with lab lat long
main_df.loc[(main_df["location.geometry.latitude"].isna()) & (
    main_df["event.test.testingLab.latitude"].notna()), "location.geometry.coarseness"] = "Testing Lab"
main_df.loc[(main_df["location.geometry.latitude"].isna()) & (main_df["event.test.testingLab.latitude"].notna(
)), "location.geometry.latitude"] = main_df["event.test.testingLab.latitude"]
main_df.loc[(main_df["location.geometry.longitude"].isna()) & (main_df["event.test.testingLab.longitude"].notna(
)), "location.geometry.longitude"] = main_df["event.test.testingLab.longitude"]

# Creating recordID
main_df["metadata.recordID"] = [uuid.uuid4() for _ in range(len(main_df))]

# Filter/Orderdataset

for std_var in COLUMNS:
    if std_var not in main_df.columns:
        main_df[std_var]=pd.NA

main_df = main_df[COLUMNS.keys()]

# Append with 2024 line list and de-duplicate
df=pd.read_csv("~/Desktop/pp-bbmp-line-list.csv")
main_df=main_df._append(df).drop_duplicates(subset=[col for col in df.columns if col!="metadata.recordID"])

# Create standardised version - removing PII
std_df = main_df.copy()
for col in COLUMNS:
    if COLUMNS[col]["access"] == False:
        std_df.drop(columns=[col], inplace=True)


# Export files
std_df.to_csv("bbmp-line-list.csv", index=False)
main_df.to_csv("pp-bbmp-line-list.csv", index=False)

# Upload pp, std and lab datasets to standardised buckets