import pandas as pd
import os
import re
import yaml
import numpy as np
import datetime
import boto3
from fuzzywuzzy import process
import uuid
import pandas as pd

# 00 - PREPROCESSING (preprocess.py)


def extract_test_method_with_result(*, test_method: str, result: str) -> tuple:
    """Creates separate NS1 and IgM columns with corresponding result if test_method and result variables provided

    Args:
        test_method (str): test method - IgM, NS1 or both
        result (str): whether positive or negative

    Returns:
        tuple: (NS1 result, IgM result)
    """

    if pd.isna(test_method):
        return (pd.NA, pd.NA)

    else:
        test1, test2 = ("", "")

        if re.search(r"NS1", str(test_method), re.IGNORECASE):
            test1 = result
        if re.search(r"IgM", str(test_method), re.IGNORECASE):
            test2 = result
        return (test1, test2)


def extract_test_method_without_result(*, test_method: str) -> tuple:
    """Creates separate NS1 and IgM columns with positive as default result if only test_method is provided

    Args:
        test_method (str): test method - IgM, NS1 or both

    Returns:
        tuple: (NS1 result, IgM result)
    """
    if pd.isna(test_method):
        return (pd.NA, pd.NA)

    else:
        test1, test2 = ("", "")

        if re.search(r"NS1", str(test_method), re.IGNORECASE):
            test1 = "Positive"
        if re.search(r"IgM", str(test_method), re.IGNORECASE):
            test2 = "Positive"
        return (test1, test2)


def map_columns(*, colname: str, map_dict: dict) -> str:
    """Standardises column names using mapping in config file

    Args:
        colname (str): Current column in DataFrame
        map (dict): Dictionary mapping of preprocessed col names to standardised col names

    Returns:
        str: Standardised column name
    """
    assert isinstance(colname, str) and isinstance(
        map_dict, dict), "Invalid input type for column name or dictionary"

    colname = re.sub(r"[^\w\s]", "", colname.lstrip().rstrip().lower())
    colname = re.sub(r"(\s+)", " ", colname)
    colname = re.sub(r"\s", "_", colname)

    for key, values in map_dict.items():
        if colname in values:
            return key

    return colname


def extract_contact(*, address: str) -> tuple:
    """Extracts mobile number from the address/name fields and strips the name/address from the mobile number field

    Args:
        address (str): Name & Address or Address field

    Returns:
        tuple: DataFrame series of address & mobile number
    """
    if isinstance(address, str):
        mobile_present = re.search(r"(9?1?\d{10})", address)

        if (mobile_present):
            mobile_number = mobile_present.group(1)
            address = re.sub(r"9?1?\d{10}", "", address)
            return (address, mobile_number)
    return (address, pd.NA)


def extract_age_gender(*, agegender: str) -> tuple:
    """Extracts age and gender from a slash-separated string

    Args:
        agegender (str): age/gender field

    Returns:
        tuple: age, gender as strings
    """

    if not pd.isna(agegender):
        match = re.search(
            r"([0-9]+[YyMm]?[A-Za-z]*)\/([MmFfGgBbWw]?[A-Za-z]*)", str(agegender))
        if match:
            if match.group(1) and match.group(2):
                return (match.group(1), match.group(2))
            elif match.group(1):
                return (match.group(1), pd.NA)
            else:
                return (match.group(2), pd.NA)
        else:
            return (pd.NA, pd.NA)
    else:
        return (pd.NA, pd.NA)

# 01 - STANDARDISATION (standardise.py)


def standardise_age(*, age: str) -> float:
    """Extracts year and month from string age entries

    Args:
        age (str): age string in the raw data

    Returns:
        float: age rounded to 2 decimal places
    """
    if isinstance(age, str):
        pattern = r'^(\d+\.?\d*) *([ym]?[ |.|,|-]?.*)?$'
        match = re.search(pattern, age)
        if match:
            if match.group(1):
                if re.match(r'^\d{1,3}', match.group(1)):
                    age = float(match.group(1))
                else:
                    return pd.NA
            else:
                return pd.NA
            if match.group(2):
                if re.match('^[m|M].*', match.group(2)):
                    if age < 13:
                        return round(age / 12, 2)
                    else:
                        return age
                elif re.match(r'^[y|Y]\D*\d{1,2}[m|M]', match.group(2)):
                    month_match = re.match(
                        r'^[y|Y]\D*(\d{1,2})[m|M]', match.group(2))
                    if month_match.group(1):
                        month = round(float(month_match.group(1)) / 12, 2)
                        age += month
                        return age
                else:
                    return age
            return age
        else:
            return pd.NA
    elif isinstance(age, int):
        return float(age)
    elif isinstance(age, float):
        return age
    else:
        return pd.NA


def validate_age(*, age, upper_limit=105):
    """Validates age range

    Args:
        age: Age (as float/NaT)
        upper_limit(int): Upper limit for age

    Returns:
        float/NaT: <0 Age <106 
    """
    if isinstance(age, float):
        if age > 0 and age <= upper_limit:
            return age
        elif age > 0:
            return age//10
        else:
            return pd.NA
    else:
        return age


def standardise_gender(*, gender: str) -> str:
    """Standardises gender

    Args:
        gender (str): gender entries in the raw dataset

    Returns:
        str: FEMALE, MALE, UNKNOWN
    """

    gender = str(gender).upper().lstrip().rstrip()

    if re.search(r'[fwgFWG]', gender):
        return "FEMALE"
    elif re.search(r'^[mbMB]', gender):
        return 'MALE'
    else:
        return 'UNKNOWN'


def standardise_test_result(*, result: str) -> str:
    """Standardises results to positive or negative

    Args:
        result (str): Result in the raw dataset

    Returns:
        str: Negative, Positive or Unknown
    """
    if isinstance(result, str) or isinstance(result, int):
        if re.search(r"-ve|Neg|Negative|No|0", str(result), re.IGNORECASE):
            return "NEGATIVE"
        elif re.search(r"NS1|IgM|D|Yes|\+ve|Pos|Positive|1", str(result), re.IGNORECASE):
            return "POSITIVE"
    return "UNKNOWN"


def generate_test_count(*, test1: str, test2: str) -> int:
    """Generates test count from test result variables

    Args:
        test1 (str): result from test 1 - positive, negative or unknown
        test2 (str): result from test 2 - positive, negative or unknown

    Returns:
        int: number of test results known - 0, 1 or 2
    """

    if test1 != "UNKNOWN" and test2 != "UNKNOWN":
        return 2
    elif test1 != "UNKNOWN" or test2 != "UNKNOWN":
        return 1
    else:
        return 0


def opd_ipd(*, s: str) -> str:
    """Standardises entries for IPD or OPD

    Args:
        s (str): IPD/OPD field in the dataset

    Returns:
        str: standardised value for IPD or OPD
    """

    if isinstance(s, str):
        if re.search(r"IPD?", s, re.IGNORECASE):
            return "IPD"
        elif re.search(r"OPD?", s, re.IGNORECASE):
            return "OPD"
        else:
            return pd.NA


def public_private(*, s: str) -> str:
    """Standardises entries for private or public

    Args:
        s (str): Private/Public field in the dataset

    Returns:
        str: standardised value for Private or Public
    """

    if isinstance(s, str):
        if re.search(r"Private|Pvt", s, re.IGNORECASE):
            return "PRIVATE"
        elif re.search(r"Public|Pub|Govt|Government", s, re.IGNORECASE):
            return "PUBLIC"
        else:
            return pd.NA


def active_passive(*, s: str) -> str:
    """Standardises entries for active or passive

    Args:
        s (str): Active/Passive field in the dataset

    Returns:
        str: standardised value for Active or Passive
    """

    if isinstance(s, str):
        if re.search(r"Acti?v?e?|A", s, re.IGNORECASE):
            return "ACTIVE"
        elif re.search(r"Pas?s?i?v?e?|P", s, re.IGNORECASE):
            return "PASSIVE"
        else:
            return pd.NA


def rural_urban(*, s: str) -> str:
    """Standardises entries for rural or urban

    Args:
        x (str): Rural/Urban field in the dataset

    Returns:
        str: standardised value for Rural or Urban
    """

    if isinstance(s, str):
        if re.search(r"Rura?l?|R", s, re.IGNORECASE):
            return "RURAL"
        elif re.search(r"Urba?n?|U", s, re.IGNORECASE):
            return "URBAN"
        else:
            return pd.NA


def fix_symptom_date(*, symptomDate: str, resultDate: str) -> datetime.datetime:
    """If symptom date is in number of days, extracts number and converts to date as result date - number

    Args:
        symptomDate (str): symptom date as date string/integer
        resultDate (str): result date as date string
    """

    if isinstance(symptomDate, str) and (isinstance(resultDate, str) or isinstance(resultDate, datetime.datetime)):
        match = re.search(r".*(\d+)\s?(days?)", symptomDate, re.IGNORECASE)
        if match:
            if match.group(1):
                try:
                    resultDate = pd.to_datetime(resultDate)
                    symptomDate = resultDate - \
                        pd.to_timedelta(int(match.group(1)), unit='d')
                except ValueError:
                    return (pd.NA, pd.NA)
            else:
                try:
                    resultDate = pd.to_datetime(resultDate)
                    return (pd.NA, resultDate)
                except ValueError:
                    return (pd.NA, pd.NA)
        else:
            return (symptomDate, resultDate)

    return (symptomDate, resultDate)


def string_clean_dates(*, Date) -> datetime:
    """Nullifies dates with no number, cleans extraneous elements in dates, and converts to datetime format

    Args:
        Date (str or datetime or NaT): date in dataset

    Returns:
        datetime: date in datetime format 
    """

    if not re.search(r"\d", str(Date)):
        return pd.NA
    else:
        date = re.sub(r"\-\-", "-", str(Date))
    try:
        date = pd.to_datetime(Date, format="mixed")
        return Date
    except ValueError:
        return pd.NA


def fix_year_hist(*, Date: datetime.datetime, Year: int) -> datetime.datetime:
    """Fixes year to current year/next year/previous year where year is not equal to the current year

    Args:
        Date (datetime.datetime): date variable in datetime format
        Year (int): year if the file

    Returns:
        tuple: clean date with year = current/next/previous
    """

    if pd.isna(Date):
        return (pd.NA)

    assert isinstance(Date, datetime.datetime) and isinstance(
        Year, int), "Input date and int year"

    # if first date is not null, and year is not current year
    if Date.year != Year:
        # set year to current year if month is not Jan or Dec
        if Date.month != 1 and Date.month != 12:
            Date = datetime.datetime(
                day=Date.day, month=Date.month, year=Year)
        else:
            # if month is Jan or Dec, calculate the diff b/w the year and current year
            year_diff = (Date.year - Year)
            # if diff greater than 1 - i.e., not from previous or next year, set year to current year
            if abs(year_diff) > 1:
                Date = datetime.datetime(
                    day=Date.day, month=Date.month, year=Year)
            # if date is from previous or next year -
            # if month is dec, set to previous year
            elif Date.month == 12:
                Date = datetime.datetime(
                    day=Date.day, month=Date.month, year=Year-1)
            # else (month is jan), set to next year
            else:
                Date = datetime.datetime(
                    day=Date.day, month=Date.month, year=Year+1)

    return (Date)


def fix_two_dates(*, earlyDate: datetime.datetime, lateDate: datetime.datetime) -> tuple:
    """Fixes invalid year entries, and attempts to fix logical check on symptom date>=sample date>=result date through date swapping

    Args:
        earlyDate (datetime): First date in sequence (symptom date or sample date)
        lateDate (datetime): Second date in sequence (sample date or result date)

    Returns:
        tuple: If logical errors can be fixed, returns updated date(s). Else, returns original dates.
    """

    assert (isinstance(lateDate, datetime.datetime) or pd.isna(lateDate)) and (isinstance(
        earlyDate, datetime.datetime) or pd.isna(earlyDate)), "Format the dates before applying this function"

    # Fix dates
    # if any of the dates is na, return dates as is
    if pd.isna(earlyDate) or pd.isna(lateDate):
        return (earlyDate, lateDate)

    delta = lateDate-earlyDate

    # if diff between second and first date is >30 or <0, attempt to fix dates
    if (pd.Timedelta(30, "d") < delta) | (delta < pd.Timedelta(0, "d")):

        # if day of second date=month of first date and day is in month-range, try swapping it's day and month
        # e.g. 2023-02-05, 2023-06-02
        if (lateDate.day == earlyDate.month) & (lateDate.day in range(1, 13)):
            newLateDate = datetime.datetime(
                day=lateDate.month, month=lateDate.day, year=lateDate.year)
            try:
                assert pd.Timedelta(0, "d") <= newLateDate - \
                    earlyDate <= pd.Timedelta(60, "d")
                return (earlyDate, newLateDate)
            except AssertionError:  # if fix doesn't yield 31> delta > 0, retain original dates
                return (earlyDate, lateDate)

        # if day of first date=month of second date and day is in month-range, try swapping it's day and month
        # e.g. 2023-06-02, 2023-02-05
        elif (earlyDate.day == lateDate.month) & (earlyDate.day in range(1, 13)):
            newEarlyDate = datetime.datetime(
                day=earlyDate.month, month=earlyDate.day, year=earlyDate.year)
            try:
                assert pd.Timedelta(0, "d") <= lateDate - \
                    newEarlyDate <= pd.Timedelta(60, "d")
                return (newEarlyDate, lateDate)
            except AssertionError:  # if fix doesn't yield 31> delta > 0, retain original dates
                return (earlyDate, lateDate)

        # if both dates have the same day and different month, try swapping day and month for both dates
        # e.g. 2023-08-02, 2023-11-02
        elif (earlyDate.day == lateDate.day) & (earlyDate.day in range(1, 13)):
            newEarlyDate = datetime.datetime(
                day=earlyDate.month, month=earlyDate.day, year=earlyDate.year)
            newLateDate = datetime.datetime(
                day=lateDate.month, month=lateDate.day, year=lateDate.year)
            try:
                assert pd.Timedelta(0, "d") <= newLateDate - \
                    newEarlyDate <= pd.Timedelta(60, "d")
                return (newEarlyDate, newLateDate)
            except AssertionError:  # if fix doesn't yield 31> delta > 0, retain original dates
                return (earlyDate, lateDate)

        # if difference between day of second date and month of first date is 1, try swapping day and month for second date
        # e.g. 2023-08-27, 2023-06-09
        elif (lateDate.day-earlyDate.month == 1) & (lateDate.day in range(1, 13)):
            newLateDate = datetime.datetime(
                day=lateDate.month, month=lateDate.day, year=lateDate.year)
            try:
                assert pd.Timedelta(0, "d") <= newLateDate - \
                    earlyDate <= pd.Timedelta(60, "d")
                return (earlyDate, newLateDate)
            except AssertionError:  # if fix doesn't yield 31> delta > 0, retain original dates
                return (earlyDate, lateDate)

        # if difference between day of first date and month of second date is -1, try swapping day and month for first date
        # e.g., 2023-10-07, 2023-08-09
        elif (earlyDate.day-lateDate.month == -1):  # standalone fix to sample date
            newEarlyDate = datetime.datetime(
                day=earlyDate.month, month=earlyDate.day, year=earlyDate.year)
            try:
                assert pd.Timedelta(0, "d") <= lateDate - \
                    newEarlyDate <= pd.Timedelta(60, "d")
                return (newEarlyDate, lateDate)
            except AssertionError:  # if fix doesn't yield 31> delta > 0, retain original dates
                return (earlyDate, lateDate)
        else:
            # returns original dates if conditions unmet
            return (earlyDate, lateDate)
    else:
        # returns original dates if dates meet logical conditions
        return (earlyDate, lateDate)


def clean_strings(*, s: str) -> str:
    """Standardises string entries

    Args:
        s (str): string entries in the raw dataset

    Returns:
        str: null for entries without  a single alphabet, no extraspaces/whitespaces, upper case 
    """

    if isinstance(s, str):
        if re.search(r'[A-Za-z]', s):
            x = re.sub(r'[\.\,\-\)\(]', ' ', s)
            x = re.sub(r'[^a-zA-Z0-9\s]+', '', s)
            x = re.sub(r'\s+', ' ', s).strip()
            return x.lstrip().rstrip().upper()
    return pd.NA


def dist_mapping(*, stateID: str, districtName: str, regions_df: pd.DataFrame, threshold: int) -> tuple:
    """Standardises district names and codes (based on LGD), provided the standardised state ID

    Args:
        stateID (str): standarised state ID
        districtName (str): raw district name
        regions_df (pd.DataFrame): regions.csv as a dataframe
        threshold (int): cut-off for fuzzy matching

    Returns:
        tuple: (LGD district name, LGD district code or admin_0 if not matched)
    """

    if pd.isna(districtName):
        return (pd.NA, pd.NA)

    districtName = districtName.upper().strip()
    districtName = re.sub(r"GULBARGA", "KALABURAGI", districtName)
    districtName = re.sub(r"\(?\sU\)?$", " URBAN", districtName)
    districtName = re.sub(r"\(?\sR\)?$", " RURAL", districtName)
    districtName = re.sub(r"BIJAPUR", "VIJAYAPURA", districtName)
    districtName = re.sub(
        r"B[AE]NGAL[OU]R[UE]\s?C?I?T?Y?|BBMP", "BENGALURU URBAN", districtName)

    districts = regions_df[regions_df["parentID"]
                           == stateID]["regionName"].to_list()
    match = process.extractOne(districtName, districts, score_cutoff=threshold)
    if match:
        districtName = match[0]
        districtCode = regions_df[(regions_df["parentID"] == stateID) & (
            regions_df["regionName"] == districtName)]["regionID"].values[0]
    else:
        districtCode = "admin_0"
    return (districtName, districtCode)  # returns original name if unmatched


def subdist_ulb_mapping(*, districtID: str, subdistName: str, regions_df: pd.DataFrame, threshold: int) -> tuple:
    """Standardises subdistrict/ulb names and codes (based on LGD), provided the standardised district ID

    Args:
        districtID (str): standarised district ID
        subdistName (str): raw subdistrict/ulb name
        regions_df (pd.DataFrame): regions.csv as a dataframe
        threshold (int): cut-off for fuzzy matching

    Returns:
        tuple: (LGD subdistrict/ulb name, LGD subdistrict/ulb code or admin_0 if not matched)
    """
    # subdist
    if pd.isna(subdistName):
        return (pd.NA, pd.NA)

    subdistName = subdistName.upper().strip()
    subdistName = re.sub(r'\(?\sU\)?$', " URBAN", subdistName, re.IGNORECASE)
    subdistName = re.sub(r'\(?\sR\)?$', " RURAL", subdistName, re.IGNORECASE)
    subdistricts = regions_df[regions_df["parentID"]
                              == districtID]["regionName"].to_list()
    match = process.extractOne(
        subdistName, subdistricts, score_cutoff=threshold)
    if match:
        subdistName = match[0]
        subdistCode = regions_df[(regions_df["parentID"] == districtID) & (
            regions_df["regionName"] == subdistName)]["regionID"].values[0]
        return (subdistName, subdistCode)
    else:
        return (subdistName, "admin_0")  # returns original name if unmatched


def village_ward_mapping(*, subdistID: str, villageName: str, regions_df: pd.DataFrame, threshold: int) -> tuple:
    """Standardises village names and codes (based on LGD), provided the standardised district ID

    Args:
        subdistID (str): standarised subdistrict/ulb ID
        villageName (str): raw village/ward name
        df (pd.DataFrame): regions.csv as a dataframe
        threshold (int): cut-off for fuzzy matching

    Returns:
        tuple: (LGD village/ward name, LGD village/ward code or admin_0 if not matched)
    """
    if pd.isna(villageName):
        return (pd.NA, pd.NA)

    villageName = villageName.upper().strip()
    villages = regions_df[regions_df["parentID"]
                          == subdistID]["regionName"].to_list()
    match = process.extractOne(villageName, villages, score_cutoff=threshold)
    if match:
        villageName = match[0]
        villageCode = regions_df[(regions_df["parentID"] == subdistID) & (
            regions_df["regionName"] == villageName)]["regionID"].values[0]
        return (villageName, villageCode)
    else:
        return (villageName, "admin_0")  # returns original name if unmatched
