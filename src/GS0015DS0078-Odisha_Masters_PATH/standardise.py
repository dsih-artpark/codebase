import pandas as pd
from epipipeline import get_regionIDs
from epipipeline.standardise.gis import (dist_mapping, subdist_ulb_mapping)
import uuid
# import raw file from path
dist_block = pd.read_excel("dist_block_chc_sc.xlsx")

# import our database of LGD codes
regionids_df, regionids_dict = get_regionIDs()


## 1 - DISTRICTS & BLOCKS
# Extract unique list of districts in path's file, excl. totals
dist_block=dist_block.drop_duplicates().reset_index(drop=True)
dist_block=dist_block[dist_block["District"].str.contains("Total")==False]
dist_block=dist_block[dist_block["District"].str.contains("Others")==False]
dist_block=dist_block[dist_block["Block"].str.contains("Total")==False]
dist_block=dist_block[dist_block["Block"].str.contains("Others")==False]
dist_block=dist_block[dist_block["CHC"].str.contains("Total")==False]
dist_block=dist_block[dist_block["CHC"].str.contains("Others")==False]
dist_block=dist_block[dist_block["Subcenter"].str.contains("Total")==False]
dist_block=dist_block[dist_block["Subcenter"].str.contains("Others")==False]

dist_block.columns

# map districts to LGD
res=dist_block.apply(lambda x: dist_mapping(stateID="state_21", districtName=x["District"], df=regionids_df), axis=1)
dist_block["location.admin2.name"], dist_block["location.admin2.ID"] = zip(*res)

# map subdistricts to LGD
res=dist_block.apply(lambda x: subdist_ulb_mapping(districtID=x["location.admin2.ID"], subdistName=x["Block"], childType = "subdistrict", df=regionids_df), axis=1)
dist_block["location.admin3.name"], dist_block["location.admin3.ID"] = zip(*res)

## 2 - ULBs
ulb=pd.read_excel("ulb.xlsx")

# Extract unique list of ulbs in path's file, excl. totals
ulb=ulb.drop_duplicates().reset_index(drop=True)
ulb=ulb[ulb["District"].str.contains("Total")==False]
ulb=ulb[ulb["District"].str.contains("Others")==False]
ulb=ulb[ulb["Name"].str.contains("Total")==False]
ulb=ulb[ulb["Name"].str.contains("Others")==False]

# map districts to LGD
res=ulb.apply(lambda x: dist_mapping(stateID="state_21", districtName=x["District"], df=regionids_df), axis=1)
ulb["location.admin2.name"], ulb["location.admin2.ID"] = zip(*res)

# map subdistricts to LGD
res=ulb.apply(lambda x: subdist_ulb_mapping(districtID=x["location.admin2.ID"], subdistName=x["Name"], childType = "ulb", df=regionids_df), axis=1)
ulb["location.admin3.name"], ulb["location.admin3.ID"] = zip(*res)

ulb = ulb[["location.admin2.ID", "location.admin2.name", "location.admin3.ID", "location.admin3.name"]]
dist_block=dist_block[["location.admin2.ID", "location.admin2.name", "location.admin3.ID", "location.admin3.name", "CHC", "Subcenter"]]

dist_block["location.admin1.ID"]="state_21"
dist_block["location.admin1.name"]="Odisha"
dist_block["location.country.ID"]="country_IN"
dist_block["location.country.name"]="India"

dist_block["metadata.recordID"]=[uuid.uuid4() for _ in range(len(dist_block))]

dist_block=dist_block.rename(columns={"CHC":"location.healthcentre.chc", 
"Subcenter":"location.healthcentre.subcentre"})

dist_block=dist_block[["metadata.recordID", "location.country.ID", "location.country.name", "location.admin2.ID", "location.admin2.name", "location.admin3.ID", "location.admin3.name",
"location.healthcentre.chc", "location.healthcentre.subcentre"]]


dist_block.to_csv("path-health-centres.csv", index=False)

blocks_ulb=dist_block[["location.country.ID", "location.country.name", "location.admin1.ID", "location.admin1.name", "location.admin2.ID", "location.admin2.name", "location.admin3.ID", "location.admin3.name"]]

blocks_ulb=blocks_ulb.drop_duplicates()

blocks_ulb=blocks_ulb._append(ulb)

blocks_ulb["location.admin1.ID"]="state_21"
blocks_ulb["location.admin1.name"]="Odisha"
blocks_ulb["location.country.ID"]="country_IN"
blocks_ulb["location.country.name"]="India"


blocks_ulb["metadata.recordID"]=[uuid.uuid4() for _ in range(len(blocks_ulb))]

blocks_ulb=blocks_ulb[["metadata.recordID", "location.country.ID", "location.country.name", "location.admin1.ID", "location.admin1.name", "location.admin2.ID", "location.admin2.name", "location.admin3.ID", "location.admin3.name"]]

blocks_ulb
blocks_ulb.to_csv("path-blocks-ulbs.csv", index=False)


## 3 - SSH
ssh=pd.read_excel("ssh.xlsx")

ssh["SSH"]=ssh["SSH"].str.replace("District Hospital", "District Hospital (DH)")

ssh.rename(columns={"SSH":"location.healthcentre.ssh"}, inplace=True) 

res=ssh.apply(lambda x: dist_mapping(stateID="state_21", districtName=x["location.healthcentre.ssh"], df=regionids_df), axis=1)
ssh["location.admin2.name"], ssh["location.admin2.ID"] = zip(*res)

ssh.loc[ssh["location.admin2.ID"].isna()==True, "location.admin2.name"]=pd.NA


ssh["location.admin1.ID"]="state_21"
ssh["location.admin1.name"]="Odisha"
ssh["location.country.ID"]="country_IN"
ssh["location.country.name"]="India"
ssh["metadata.recordID"]=[uuid.uuid4() for _ in range(len(ssh))]

ssh=ssh[["metadata.recordID", "location.country.ID", "location.country.name", "location.admin1.ID", "location.admin1.name", "location.admin2.ID", "location.admin2.name", "location.healthcentre.ssh"]]

ssh.to_csv("path-ssh.csv", index=False)