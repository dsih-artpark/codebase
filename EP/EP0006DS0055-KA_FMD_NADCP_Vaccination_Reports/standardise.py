import os
import re
import pandas as pd

from dataio.download import download_dataset_v2, fetch_data_documentation
from epipipeline.preprocess import map_column
from epipipeline.standardise.gis import dist_mapping
from epipipeline import get_regionIDs
from epipipeline.standardise.dates import string_clean_dates
import uuid

# get metadata.yaml

D=fetch_data_documentation(dsid="EP0006DS0055")
D=D["tables"]["nadcp-vaccination-progress"]
data_dictionary = D["data_dictionary"]
header_mapper = D["admin"]["header_mapper"]

# get preprocessed data
folder_path = download_dataset_v2(dsid="EP0006DS0055", data_state = "preprocessed")

folder_path = os.path.join("data", folder_path)

# get regionIDs

regionids_df, regionids_dict = get_regionIDs()

# clean int values
def clean_int(x):
    if pd.isna(x):
        return pd.NA
    
    x = re.sub("[^0-9]", "", str(x))
    try:
        return float(x)
    except:
        return pd.NA
         

# iterate through files and standardise

standardised_dict={}


# iterate through folders for rounds 1-5
for folder in os.listdir(folder_path):
    if not folder.startswith("."):
        # extract round from folder name
        round_present = re.search(r"round(\d)", folder)
        if round_present:
            round = round_present.group(0)
        else:
            round = pd.NA
            print(f"round missing for {folder}")
        
        # consolidate all files within a round into a dataframe
        main_df=pd.DataFrame()

        file_path = os.path.join(folder_path, folder)

        for file in os.listdir(file_path):
            if file.endswith(".csv"):
                df=pd.read_csv(f"{file_path}/{file}")
                
                # rename cols
                standard_mapper = map_column(map_dict=header_mapper)
                df = df.rename(columns=standard_mapper)

                
                # add std cols
                for col in data_dictionary:
                    if "default_value" in data_dictionary[col]:
                        df[col] = data_dictionary[col]["default_value"]
                    if col not in df.columns:
                        df[col] = pd.NA

                # filter cols

                df=df[data_dictionary.keys()]

                # extract report date from file
                date_in_file = re.search(r"(\d\d?\-\d\d?-\d\d\d\d)", str(file))
                if not date_in_file:
                    date=pd.NA
                    print(f"{file} in {folder} does not have date in filename")
                else:
                    date = date_in_file.group(0)
                    try:
                        date = pd.to_datetime(date_in_file.group(0), dayfirst=True)
                    except:
                        date = date_in_file.group(0)
                    
                # set report date and round
                df["metadata.reportDate"] = date
                df["metadata.round"] = round
                
                # clean int cols
                for col in df.columns:
                    if col.startswith("daily") or col.startswith("cumulative"):
                        df[col] = df[col].apply(lambda x: clean_int(x))
                    
                # clean dist name
                df["location.admin2.name"]=df["location.admin2.name"].str.strip().str.title()
                df.loc[df["location.admin2.name"]=="Bangalore", "location.admin2.name"]="Bengaluru Urban"

                # map dist
                res=df.apply(lambda x: dist_mapping(stateID=x["location.admin1.ID"], districtName=x["location.admin2.name"], df=regionids_df), axis=1)
                df["location.admin2.name"], df["location.admin2.ID"] = zip(*res)
                
                # fix_date_vars
                for var in ['metadata.vaccinationStartDate','metadata.reportDate' ]:
                    df[var]=df[var].apply(lambda x: string_clean_dates(Date=x))
                    df[var]=df[var].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

                df=df.drop_duplicates()

                df=df.dropna(subset=["location.admin2.name"]).dropna(how="all", axis=0)

                df["metadata.recordID"] = [uuid.uuid4() for _ in range(len(df))]

                main_df=main_df._append(df)

        standardised_dict[round]=main_df

# export to csvs and then manually upload to aws standardised bucket
for key in standardised_dict:
    df = standardised_dict[key]
    df.to_csv(f"standardised/{key}.csv", index=False)
