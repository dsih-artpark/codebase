#!/bin/bash

# Paths to AWS S3 raw folders where data is to be uploaded
bucket="dsih-artpark-01-raw-data"
# to be suffixed with year
ll_prefix="EPRDS7-KA_Dengue_Chikungunya_LL"
# to be suffixed with year
sum_prefix="EPRDS8-KA_Dengue_Chikungunya_SUM"

# If User input 1 is provided, pull directory from there, else default dir is set to Downloads
if [ $# -gt 0 ]; then
    dir=$1
elif ls "${HOME}/Desktop" | grep -q "ka-dengue"; then
    dir="${HOME}/Desktop/ka-dengue/"
else
    echo "Create directory ka-dengue and provide full user access. EXIT." | tee -a "$log"
    exit 1
fi

# Removing existing .xlsx files
rm *.xlsx
~
# Find the latest added zip folder starting with "dailyreportof" (if multiple, else picks up only folder)
latest_folder=$(ls -t "${dir}" | grep "zip$" | head -n 1)

# If no folder is found, log it and exit
if [ -z "$latest_folder" ]; then
    echo "$(date): No dailyreport zip folders found. EXIT." | tee -a "$log"
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
        echo "Invalid date format. Expected format: yyyy-dd-mm. EXIT." | tee -a "$log"
        exit 1
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

# #Extract the zip folder into a folder with the same name, excl. .zip extension
unzip -q "${dir}${latest_folder}"

unzipped_folder=${dir}${latest_folder%.*}

# Create an array with the list of files in the unzipped directory
files=()
while IFS= read -r -d '' file; do
    file=$(basename "$file")
    files+=("$file")
done < <(find . -maxdepth 1 -type f -name "*.xlsx" -print0)

if [ ${#files[@]} -ne 5 ]; then
  echo "Number of files in dir is not 5. Check for missing files/remove extra files." | tee -a "$log"
fi

count=0

# Rename, upload, tag for LL and summ
for file in "${files[@]}"; do
    if echo "$file" | grep -q '^A-1'; then
        new_name="${current_year}A1.xlsx"
        mv "$file" "$new_name" && aws s3 cp "$new_name" "s3://${bucket}/${ll_prefix}/${current_year}/${new_name}" && aws s3api put-object-tagging --bucket "${bucket}" --key "${ll_prefix}/${current_year}/${new_name}" --tagging "TagSet=[{Key=${tag_key},Value=''}]" && echo "$new_name uploaded and tagged" | tee -a "$log" 2>> "$log"  && ((count++))
    elif echo "$file" | grep -q '^B-1'; then
        new_name="${current_year}B1.xlsx"
        mv "$file" "$new_name" && aws s3 cp "$new_name" "s3://${bucket}/${ll_prefix}/${current_year}/${new_name}" && aws s3api put-object-tagging --bucket "${bucket}" --key "${ll_prefix}/${current_year}/${new_name}" --tagging "TagSet=[{Key=${tag_key},Value=''}]" && echo "$new_name uploaded and tagged" | tee -a "$log" 2>> "$log" && ((count++))
    elif echo "$file" | grep -q '^A-2'; then
        new_name="${current_year}A2.xlsx"
        mv "$file" "$new_name" && aws s3 cp "$new_name" "s3://${bucket}/${ll_prefix}/${current_year}/${new_name}" && aws s3api put-object-tagging --bucket "${bucket}" --key "${ll_prefix}/${current_year}/${new_name}" --tagging "TagSet=[{Key=${tag_key},Value=''}]" && echo "$new_name uploaded and tagged" | tee -a "$log" 2>> "$log" && ((count++))
    elif echo "$file" | grep -q '^B-2'; then
        new_name="${current_year}B2.xlsx"
        mv "$file" "$new_name" && aws s3 cp "$new_name" "s3://${bucket}/${ll_prefix}/${current_year}/${new_name}" && aws s3api put-object-tagging --bucket "${bucket}" --key "${ll_prefix}/${current_year}/${new_name}" --tagging "TagSet=[{Key=${tag_key},Value=''}]" && echo "$new_name uploaded and tagged" | tee -a "$log" 2>> "$log" && ((count++))
    elif echo "$file" | grep -q '^[0-9]'; then
        new_name="${current_year}-${current_month}.xlsx"
        mv "$file" "$new_name" && aws s3 cp "$new_name" "s3://${bucket}/${sum_prefix}/${current_year}/${new_name}" && aws s3api put-object-tagging --bucket "${bucket}" --key "${sum_prefix}/${current_year}/${new_name}" --tagging "TagSet=[{Key=${tag_key},Value=''}]" && echo "$new_name uploaded and tagged" | tee -a "$log" 2>> "$log" && ((count++))
    else 
        echo "Skipping file '$file' - does not match any pattern - start with A-1, B-1, A-2, B-2 or in format 06 - June 2024 F24. Correct format to startin" | tee -a "$error_log"
        continue
    fi
done

if [ "$count" -ge 4 ]; then
    rm .*zip
fi