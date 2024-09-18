import pandas as pd
import re


def set_headers(df: pd.DataFrame, pivot_column: str, col_start_index: int, col_start_value) -> pd.DataFrame:
    """Set the correct headers for the DataFrame and clean up the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        pivot_column (str): Name of the stable column used to identify the header.
        col_start_index (int): Index of the column used to identify the dataframe start row (start at 0).
        col_start_value: Value in col_start_index column indicating the start of dataframe.

    Returns:
        pd.DataFrame: DataFrame with correct headers set.
    """
    assert isinstance(
        df, pd.DataFrame), "Invalid input: df must be a DataFrame"
    assert isinstance(
        pivot_column, str), "Invalid input: pivot_column must be a string"
    assert isinstance(
        col_start_index, int), "Invalid input: col_start_index must be an integer"
    assert 0 <= col_start_index < len(
        df.columns), "Invalid input: col_start_index out of range"

    def search_header(L: list, pivot_col_name: str) -> bool:
        """Identify if the current list of DataFrame headers contains the pivot column.

        Args:
            L (list): Current list of DataFrame headers.
            pivot_col_name (str): Column name used as a reference to identify the header.

        Returns:
            bool: Whether the pivot column is not found in the current headers.
        """
        assert isinstance(L, list) and isinstance(
            pivot_col_name, str), "Invalid input"

        pivot_col_name = pivot_col_name.strip()
        for column in L:
            if re.search(pivot_col_name, str(column), re.IGNORECASE):
                return False
        return True

    # Find the correct header row
    i = 0
    while search_header(list(df.columns), pivot_column) and (i < 6):  # Adjust as needed
        df.columns = df.iloc[i, :]
        i += 1

    # Drop the rows before the identified header row
    df = df.drop(index=range(i)).reset_index(drop=True)

    # Forward fill for NaN or unnamed columns
    for j in range(1, len(df.columns)):
        if re.search("Unnamed", str(df.columns[j]), re.IGNORECASE) or pd.isna(df.columns[j]):
            df.columns.values[j] = df.columns.values[j-1]

    # Identify where data starts based on a column and value input
    start_index = df[df.iloc[:, col_start_index] == col_start_value].index[0]

    # Upward fill merged columns if necessary
    for row in range(start_index):
        row_data = df.iloc[row].tolist()
        for j in range(len(row_data)):
            if not pd.isna(row_data[j]):
                merge_col = re.sub(
                    r"[\,\.\-\d\(\)\s\*\-\_]+", "", str(row_data[j])).lower()
                df.columns.values[j] += merge_col

    # Drop the rows before the data starts
    df = df.drop(index=range(start_index + 1)).reset_index(drop=True)

    return df

# Usage example:
# df = pd.read_csv("your_data.csv")
# processed_df = set_headers(df, pivot_column="YourPivotColumn", col_start_index=0, col_start_value="YourStartValue")
