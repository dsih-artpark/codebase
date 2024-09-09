import pandas as pd
from dataio.download import fetch_data_documentation, download_dataset_v2
from epipipeline.standardise.gis import dist_mapping, village_ward_mapping, subdist_ulb_mapping, clean_lat_long
from epipipeline.preprocess import clean_colname, map_column
from epipipeline.standardise import clean_strings
from epipipeline import get_regionIDs
import uuid
import boto3

# Fetch raw data
rds_path = download_dataset_v2(dsid='EPRDS41', data_state='raw')
df=pd.read_excel(f"data/{rds_path}outbreaks.xlsx", skiprows=1)

# Fetch regionids
download_dataset_v2(dsid='GS0015DS0034', data_state='standardised')
regionID_df, regionID_dict = get_regionIDs()

# Fetch metadata.yaml
D = fetch_data_documentation(dsid='EP0006DS0075')
DATA_DICTIONARY = D["tables"]["outbreaks"]["data_dictionary"]
HEADER_MAPPER = D["tables"]["outbreaks"]["admin"]["header_mapper"]
BUCKET_NAME = D["tables"]["outbreaks"]["admin"]["config"]["upload"]["bucket"]
FILE_NAME = D["tables"]["outbreaks"]["admin"]["config"]["upload"]["file_name"]
KEY = D["tables"]["outbreaks"]["admin"]["dsid"]["standardised"]+"-"+D["tables"]["outbreaks"]["admin"]["config"]["upload"]["prefix"]+"/"+FILE_NAME



# Standardise colnames
## std strings
cols=[clean_colname(colname=col) for col in df.columns]
df.columns = cols

## map to std headeers
STANDARD_MAPPER = map_column(map_dict = HEADER_MAPPER)
df = df.rename(columns = STANDARD_MAPPER)

# add std headers
for std_header in DATA_DICTIONARY.keys():
    if "default_value" in DATA_DICTIONARY[std_header].keys():
        df[std_header]=DATA_DICTIONARY[std_header]["default_value"]
    else:
        if std_header not in df.columns:
            df[std_header]=pd.NA

# filter/order cols
df = df[DATA_DICTIONARY.keys()]

# clean lat, long
res=df.apply(lambda x: clean_lat_long(lat=x["location.geometry.latitude"], long=x["location.geometry.longitude"]), axis=1)

df["location.geometry.latitude"], df["location.geometry.longitude"] = zip(*res)

for col in ["location.admin2.name", "location.admin3.name", "location.admin5.name"]:
    df[col]=df[col].apply(lambda x: clean_strings(s=x))
    
# map districts
res=df.apply(lambda x: dist_mapping(stateID=x["location.admin1.ID"], districtName=x["location.admin2.name"], df=regionID_df), axis=1)
df["location.admin2.name"], df["location.admin2.ID"] = zip(*res)

# map subdistricts
res=df.apply(lambda x: subdist_ulb_mapping(districtID=x["location.admin2.ID"], subdistName=x["location.admin3.name"], df=regionID_df), axis=1)
df["location.admin3.name"], df["location.admin3.ID"] = zip(*res)

# map villages
res=df.apply(lambda x: village_ward_mapping(subdistID=x["location.admin3.ID"], villageName=x["location.admin5.name"], df=regionID_df), axis=1)
df["location.admin5.name"], df["location.admin5.ID"] = zip(*res)

# update admin_hierarchy
df["location.admin.hierarchy"]=df["location.admin3.ID"].apply(lambda x: pd.NA if pd.isna(x) else ("Subdistrict" if x.startswith("subdistrict") else "ULB"))

# record ID
df["metadata.recordID"] = [uuid.uuid4() for _ in range(len(df))]

# int cols
for col in ['summary.outbreaks', 'summary.villagesAffected',
       'summary.birdsAffected', 'summary.deaths', 'summary.riskPopulation']:
    df[col] = df[col].astype(float)


df.to_csv("mh-avian-outbreaks.csv", index=False)

# upload to s3
s3 = boto3.resource("s3")

try:
    s3.meta.client.upload_file(Filename = FILE_NAME, Bucket= BUCKET_NAME, Key = KEY)
    print("Uploaded to s3")
except Exception as e:
    print(f"Failed to upload to s3 - {e}")


# The End!