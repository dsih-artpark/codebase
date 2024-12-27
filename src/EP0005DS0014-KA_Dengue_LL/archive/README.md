## README for ka-dengue-upload.sh

### Description
`ka-dengue-upload.sh` is a bash script that:
- Unzips the data folder downloaded from emails sent by GoK.
- Renames the files to match the input format for preprocessing and standardization pipelines.
- Uploads the files to their respective raw data folders on AWS.

### Before Running the Code
- Ensure you have write access to `EPRDS8` and `EPRDS7`.
- Authenticate your AWS profile (default) in VS Code.
- Download the files shared by GoK via email (JD (NVBDCP) - KARNATAKA STATE <jd.mf.kar@gmail.com>) as a zip. Clicking "download" in the email will automatically download the files as a zip.
- Ensure there are 5 files: Line lists (A1, A2, B1, B2) and one file for summaries named by the month. Note: The code will still run if there are fewer than 5 files.

### Script Input Parameters
- The script takes two inputs:
  - The **first positional input** is the path where you have stored the zip folder (i.e., where you want the extraction to be done).
  - The **second positional input** is the date in `yyyy-mm-dd` format.

#### Running the Script
- **No inputs:** Run the script as:
  ```bash
  ./ka-dengue-upload.sh
