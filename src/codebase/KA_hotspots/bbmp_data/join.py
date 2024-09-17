import pandas as pd

import os

main_df=pd.DataFrame()
for file in os.listdir("bbmp_data"):
    if file.endswith(".xlsx"):
        df=pd.read_excel(f"bbmp_data/{file}")

        main_df=main_df._append(df)


df1=pd.read_csv("bbmp_data/bbmp_24to11.csv")

main_df=df1._append(main_df)

main_df.to_csv("bbmp_data/bbmp_24to13.csv", index=False)