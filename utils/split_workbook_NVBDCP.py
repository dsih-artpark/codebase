import pandas as pd
import os
import re
import datetime


def split_workbook_NVBDCP(workbook_name: str) -> bool:
    """This function splits the NVBDCP Raw Line list into individual csvs by source.

    Args:
        workbook_name (str): Name of Raw Excel Workbook

    Returns:
        bool: Whether all sheets have been processed
    """

    assert isinstance(workbook_name, str) and re.search(
        ".xls", workbook_name), "Invalid input"

    processed = True

    wb = pd.ExcelFile(workbook_name)

    for sheet in wb.sheet_names:
        if re.search("PCMC", sheet, re.IGNORECASE):
            df = pd.read_excel(workbook_name, sheet_name=sheet)
            path = os.path.join(os.curdir, "PCMC")
            try:
                os.mkdir(path)
            except FileExistsError:  # if directory exists, just save file
                df.to_csv(os.path.join(path, sheet+".csv"), index=False)
            else:
                df.to_csv(os.path.join(path, sheet+".csv"), index=False)

        elif re.search("PMC", sheet, re.IGNORECASE):
            df = pd.read_excel(workbook_name, sheet_name=sheet)
            path = os.path.join(os.curdir, "PMC")
            try:
                os.mkdir(path)
            except FileExistsError:  # if directory exists, just save file
                df.to_csv(os.path.join(path, sheet+".csv"), index=False)
            else:
                df.to_csv(os.path.join(path, sheet+".csv"), index=False)

        elif re.search("PR|Rural", sheet, re.IGNORECASE):
            df = pd.read_excel(workbook_name, sheet_name=sheet)
            path = os.path.join(os.curdir, "Pune Rural")
            try:
                os.mkdir(path)
            except FileExistsError:  # if directory exists, just save file
                df.to_csv(os.path.join(path, sheet+".csv"), index=False)
            else:
                df.to_csv(os.path.join(path, sheet+".csv"), index=False)
        else:
            processed = False
            log = open("error_log.txt", "a")
            log.write(f"\nDateTime:{datetime.datetime.now()}")
            log.write(f"\nCheck source for sheet {
                      sheet}, and process manually.\n")
            log.close()
    return (processed)
