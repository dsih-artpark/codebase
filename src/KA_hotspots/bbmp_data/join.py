# get data from dsih-artpark-01-raw-data/EPRDS2-Bengaluru_City_Dengue_LL/consolidated/

import pandas as pd

import os

main_df=pd.DataFrame()
for file in os.listdir("bbmp_data"):
    if file.endswith(".xlsx"):
        df=pd.read_excel(f"bbmp_data/{file}")

        main_df=main_df._append(df)

print(main_df.columns)

df1=pd.read_csv("bbmp_data/bbmp_aug24tosep16.csv")

print(df1.columns)

print("Check col names before appending")

main_df=df1._append(main_df)

# change manually
main_df.to_csv("bbmp_data/bbmp_aug24tosep17.csv", index=False)