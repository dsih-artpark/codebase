from typing import Optional
from pathlib import Path
import os
import logging

# Set up logging
logger = logging.getLogger("epipipeline.upload.dengue.karnataka")

logging.captureWarnings(True)

def upload_ka_dengue_files(*, 
    base_dir: Optional[str] = None,
    file_date: Optional[str] = None):

    # validate dir if provided or set cd to default
    if base_dir:

        if not os.path.isdir(base_dir):
            logger.warning(f"Directory provided as input: {base_dir} does not exist")
            raise ValueError(f"Directory provided as input: {base_dir} does not exist. Terminating..")
    else:
        logging.info("Defaulting to searching for the zip folder in the current directory as file directory was not provided as input.")
        dir_path = os.getcwd()

   # search for the most recently added zip folder
    all_zip_folders = sorted(Path(base_dir).glob("*.zip"), key=os.path.getmtime, reverse=True)

    if len(all_zip_folders)>0:
        zip_folder = all_zip_folders[0]
        logging.debug(f"Unzipping the latest zip folder {zip_folder}")
    else:
        logging.warning(f"No zip folders found in {base_dir}. Terminating..")
        exit(1)
    
    # Validate user provided date or extract from folder name
    if file_date:
        try:
            file_date=pd.to_datetime(file_date)
            logging.info(f"File date inputted is: {file_date}")
        except ValueError as e:
            raise e(f"Invalid date entered, re-enter date")
    else: # extract from folder name
        folder_date=re.search(r"\d{8}", zip_folder)
        if folder_date:
            file_date=folder_date.group(0)
            try:
                file_date=pd.to_datetime(file_date)
                logging.info(f"File date extracted is: {file_date}")
            except ValueError as e:
                raise e(f"Failed to extract date from folder name. Ensure it is in ddmmyyyy format")


    # Unzip the folder and save it in the base dir
    unzipped_folder_path = Path(base_dir) / zip_folder.stem
    unzipped_folder_path.mkdir(exist_ok=True)
    shutil.unpack_archive(str(zip_folder), unzipped_folder_path)
    logging.debug(f"Unzipped folder located at {unzipped_folder_path}.")

    tag_key = f"{file_date.year}-{file_date.month}-{file_date.day}"
    unzipped_folder = Path(base_dir) / zip_folder.stem
    unzipped_folder.mkdir(exist_ok=True)
    unzip_folder(zip_folder, unzipped_folder)
    
    files = list(unzipped_folder.glob("*.xlsx"))
    if len(files) != 5:
        logging.warning(f"Number of files is not 5. Check for missing or extra files. Terminating.")
        exit(1)
    
    count = 0
    for file in files:
        file_name = file.name
        new_name = None
        if re.match(r"^A-1", file_name):
            new_name = f"{year}A1.xlsx"
        elif re.match(r"^B-1", file_name):
            new_name = f"{year}B1.xlsx"
        elif re.match(r"^A-2", file_name):
            new_name = f"{year}A2.xlsx"
        elif re.match(r"^B-2", file_name):
            new_name = f"{year}B2.xlsx"
        elif re.match(r"^[0-9A-Za-z]", file_name):
            new_name = f"{year}-{month}.xlsx"
        
        if new_name:
            new_path = unzipped_folder / new_name
            file.rename(new_path)
            # add upload
            count += 1
        else:
            logger.warning(f"Skipping file '{file_name}' - does not match any pattern.")
    


        



