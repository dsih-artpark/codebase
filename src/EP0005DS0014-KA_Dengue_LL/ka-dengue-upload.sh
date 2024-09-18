#!/bin/bash

# Paths to AWS S3 raw folders where data is to be uploaded
bucket="dsih-artpark-01-raw-data"
ll_prefix="EPRDS7-KA_Dengue_Chikungunya_LL"
sum_prefix="EPRDS8-KA_Dengue_Chikungunya_SUM"

# Ensure log file path is set
log="${HOME}/upload_log.txt"
error_log="${HOME}/upload_error_log.txt"

# If User input 1 is provided, pull directory from there, elif ka-dengue dir is present use that, else make the dir

# If User input 1 is not an empty string, use it as the directory
if [ -n "$1" ]; then
    # User input 1 is not empty, so use it as the directory
    dir=$1
else
    # Check if the "ka-dengue" directory exists on the Desktop
    if [ -d "${HOME}/Desktop/ka-dengue/" ]; then
        # If it exists, set it as the directory
        dir="${HOME}/Desktop/ka-dengue/"
    else
        # If not, create the directory and set it
        echo "Input the directory where the zip folder is, or create a directory "ka-dengue" in your Desktop. EXIT."  | tee -a "$log"
        exit 1
    fi
fi

# Check if the directory exists
if [ ! -d "$dir" ]; then
    echo "Directory $dir does not exist. EXIT." | tee -a "$log"
    exit 1
fi

# Removing existing .xlsx files in the dir
rm "$dir"/*.xlsx 2>/dev/null

# Find the latest added zip folder
latest_folder=$(ls -t "${dir}" | grep "zip$" | head -n 1)

# If no folder is found, log it and exit
if [ -z "$latest_folder" ]; then
    echo "$(date): No dailyreport zip folders found in $dir. EXIT." | tee -a "$log"
    exit 0
fi

# If User input 2 is provided, pull date from there
if [ -n "$2" ]; then
    user_date=$2
    if [[ $user_date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        # Extract the year, month, and day correctly
        current_year=$(echo "$user_date" | cut -d '-' -f 1)
        current_month=$(echo "$user_date" | cut -d '-' -f 2)  # Field 2 for month
        current_date=$(echo "$user_date" | cut -d '-' -f 3)   # Field 3 for day (date)
    
        # Echo the extracted values to the user
        echo "Extracted Year: $current_year"
        echo "Extracted Month: $current_month"
        echo "Extracted Date: $current_date"
    else
        echo "Invalid date format. Expected format: yyyy-mm-dd."
    fi
else
    # Pull it from the latest folder name. If it doesn't exist, exit the code and log the error
    date_str=$(echo "$latest_folder" | grep -o '[0-9]\{8\}')
    if [ -n "$date_str" ]; then
        current_date=$(echo "$date_str" | cut -c 1,2)
        current_month=$(echo "$date_str" | cut -c 3,4)
        current_year=$(echo "$date_str" | cut -b 5-8)
    else
        echo "Date not found in folder name and user date not provided. EXIT." | tee -a "$log"
        exit 1
    fi

    if [ -z "$current_date" ] || [ -z "$current_month" ] || [ -z "$current_year" ]; then
        echo "Date, month, year could not be extracted from folder name. EXIT." | tee -a "$log"
        exit 1
    fi
fi

tag_key="${current_year}-${current_month}-${current_date}"

# Create a new folder with the same name as the zip file but without the extension
unzipped_folder="${dir}${latest_folder%.zip}/"
mkdir -p "$unzipped_folder"

# Extract the zip file into this new folder
unzip -q "${dir}${latest_folder}" -d "$unzipped_folder"

unzipped_folder=${dir}${latest_folder%.*}

# Create an array with the list of files in the unzipped directory
files=()
while IFS= read -r -d '' file; do
    file=$(basename "$file")
    files+=("$file")
done < <(find "$unzipped_folder" -maxdepth 1 -type f -name "*.xlsx" -print0)

# Check if the number of files is exactly 5
if [ ${#files[@]} -ne 5 ]; then
    echo "Number of files in dir is not 5. Check for missing files/remove extra files." | tee -a "$log"
    exit 1
fi

count=0

# Rename, upload, tag for LL and SUM files
for file in "${files[@]}"; do
    if echo "$file" | grep -q '^A-1'; then
        new_name="${current_year}A1.xlsx"
        mv "${unzipped_folder}/$file" "${unzipped_folder}/$new_name"
        aws s3 cp "${unzipped_folder}/$new_name" "s3://${bucket}/${ll_prefix}/${current_year}/$new_name"
        aws s3api put-object-tagging --bucket "$bucket" --key "${ll_prefix}/${current_year}/$new_name" --tagging "TagSet=[{Key=${tag_key},Value=''}]"
        ((count++))
    elif echo "$file" | grep -q '^B-1'; then
        new_name="${current_year}B1.xlsx"
        mv "${unzipped_folder}/$file" "${unzipped_folder}/$new_name"
        aws s3 cp "${unzipped_folder}/$new_name" "s3://${bucket}/${ll_prefix}/${current_year}/$new_name"
        aws s3api put-object-tagging --bucket "$bucket" --key "${ll_prefix}/${current_year}/$new_name" --tagging "TagSet=[{Key=${tag_key},Value=''}]"
        ((count++))
    elif echo "$file" | grep -q '^A-2'; then
        new_name="${current_year}A2.xlsx"
        mv "${unzipped_folder}/$file" "${unzipped_folder}/$new_name"
        aws s3 cp "${unzipped_folder}/$new_name" "s3://${bucket}/${ll_prefix}/${current_year}/$new_name"
        aws s3api put-object-tagging --bucket "$bucket" --key "${ll_prefix}/${current_year}/$new_name" --tagging "TagSet=[{Key=${tag_key},Value=''}]"
        ((count++))
    elif echo "$file" | grep -q '^B-2'; then
        new_name="${current_year}B2.xlsx"
        mv "${unzipped_folder}/$file" "${unzipped_folder}/$new_name"
        aws s3 cp "${unzipped_folder}/$new_name" "s3://${bucket}/${ll_prefix}/${current_year}/$new_name"
        aws s3api put-object-tagging --bucket "$bucket" --key "${ll_prefix}/${current_year}/$new_name" --tagging "TagSet=[{Key=${tag_key},Value=''}]"
        ((count++))
    elif echo "$file" | grep -q '^[0-9A-Za-z]'; then
        new_name="${current_year}-${current_month}.xlsx"
        mv "${unzipped_folder}/$file" "${unzipped_folder}/$new_name"
        aws s3 cp "${unzipped_folder}/$new_name" "s3://${bucket}/${sum_prefix}/${current_year}/$new_name"
        aws s3api put-object-tagging --bucket "$bucket" --key "${sum_prefix}/${current_year}/$new_name" --tagging "TagSet=[{Key=${tag_key},Value=''}]"
        ((count++))
    else
        echo "Skipping file '$file' - does not match any pattern." | tee -a "$error_log"
    fi
done

# If all 5 files were processed, remove the zip files and the unzipped folder
if [ "$count" -ge 5 ]; then
    echo "All 5 files processed. Clear the folder after manually checking the file names."
else
    echo "Fewer than 5 files processed. Manually check which files have not been renamed/uploaded to S3"
fi
