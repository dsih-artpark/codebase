#!/bin/bash

## INSTRUCTIONS FOR RUNNING THE SCRIPT 
## ARGS ARE OPTIONAL:
## FIRST ARG = Path to zip to be provided if not Downloads 
## SECOND ARG = File/email date to be provided if not in zip folder name

## e.g., path_to_script.sh path_to_zip_dir.sh yyyy-mm-dd 

# Explicitly prints outputs
set -x

# Create a log file
log="${HOME}/ka-dengue-upload-log.txt"

# Paths to AWS S3 raw folders where data is to be uploaded
ka_ll_path="dsih-artpark-01-raw-data/EPRDS7-KA_Dengue_Chikungunya_LL/"
ka_sum_path="dsih-artpark-01-raw-data/EPRDS8-KA_Dengue_Chikungunya_SUM/"

# If User input 1 is provided, pull directory from there, else default dir is set to Downloads
if [ $# -gt 0 ]; then
    dir=$1
else
    dir="${HOME}/Downloads"
fi

# Find the latest added zip folder starting with "dailyreportof"
latest_folder=$(ls -t "${dir}" | grep 'zip$' | grep '^dailyreportof' | head -n 1)

# If no folder is found, log it
if [ -z "$latest_folder" ]; then
    echo "$(date): No folders found." | tee -a "$log"
    exit 0
fi

# If User input 2 is provided, pull date from there
if [ $# -gt 1 ]; then
    user_date=$2
    if [[ $user_date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        current_year=$(echo $user_date | cut -d '-' -f 1)
        current_month=$(echo $user_date | cut -d '-' -f 3)
        current_date=$(echo $user_date | cut -d '-' -f 2)
    else
        echo "Invalid date format. Expected format: yyyy-dd-mm" | tee -a "$log"
        exit 1
    fi
else
    # Pull it from the latest folder name. If it doesn't exist, exit the code and log the error
    date_str=$(echo "$latest_folder" | grep -o '[0-9]\{8\}')
    if [ -n "$date_str" ]; then
        current_year=${date_str:4:4}
        current_month=${date_str:2:2}
        current_date=${date_str:0:2}
    else
        echo "Date not found in folder name and user date not provided. Exiting." | tee -a "$log"
        exit 1
    fi
fi

# Main script
# Change directory and extract the zip folder
cd "${dir}" || { echo "Failed to change dir to ${dir}" | tee -a "$log"; exit 1; }
tar -xf "${latest_folder}"
extracted_dir="${latest_folder%.*}"

# Change directory to extracted folder
cd "$extracted_dir" || { echo "Failed to change dir to ${extracted_dir}" | tee -a "$log"; exit 1; }

# Get list of files ending with .xlsx
files=$(ls | grep '.xlsx$')

# Create a dictionary of expected files to keep a counter
declare -A file_count=(["A1"]=0 ["B1"]=0 ["A2"]=0 ["B2"]=0 ["SUM"]=0)

# Renaming files using regex and upload them to AWS S3 with tag = date, month, year. If upload is successful, increment file count, else log error
for file in ${files}; do
    if echo "$file" | grep -q '^A\-1'; then
        new_name="${current_year}A1.xlsx"
        aws s3 cp "$file" "${ka_ll_path}${current_year}/${new_name}" --tagging "Key='${current_year}-${current_month}-${current_date}', Value=''" && file_count["A1"]=1 2>> "$log"
    elif echo "$file" | grep -q '^B\-1'; then
        new_name="${current_year}B1.xlsx"
        aws s3 cp "$file" "${ka_ll_path}${current_year}/${new_name}" --tagging "Key='${current_year}-${current_month}-${current_date}', Value=''" && file_count["B1"]=1 2>> "$log"
    elif echo "$file" | grep -q '^A\-2'; then
        new_name="${current_year}A2.xlsx"
        aws s3 cp "$file" "${ka_ll_path}${current_year}/${new_name}" --tagging "Key='${current_year}-${current_month}-${current_date}', Value=''" && file_count["A2"]=1 2>> "$log"
    elif echo "$file" | grep -q '^B\-2'; then
        new_name="${current_year}B2.xlsx"
        aws s3 cp "$file" "${ka_ll_path}${current_year}/${new_name}" --tagging "Key='${current_year}-${current_month}-${current_date}', Value=''" && file_count["B2"]=1 2>> "$log"
    else
        new_name="${current_year}-${current_month}.xlsx"
        aws s3 cp "$file" "${ka_sum_path}${current_year}/${new_name}" --tagging "Key='${current_year}-${current_month}-${current_date}', Value=''" && file_count["SUM"]=1 2>> "$log"
    fi
    mv "$file" "$new_name"
done

# If file counts are not equal to 1, print in stdout and append to log file
if [ ${file_count["A1"]} -eq 1 ] && [ ${file_count["B1"]} -eq 1 ] && [ ${file_count["A2"]} -eq 1 ] && [ ${file_count["B2"]} -eq 1 ] && [ ${file_count["SUM"]} -eq 1 ]; then
    echo "Folder ${extracted_dir}: LL & SUMM uploaded" 2>&1 | tee -a "$log"
    cd ..
    rm -rf "$extracted_dir"
    rm -rf "$latest_folder"
else
    echo "Folder ${extracted_dir}: Missing files in upload." 2>&1 | tee -a "$log"
fi