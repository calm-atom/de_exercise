import pandas as pd
from typing import Dict
import hashlib
from utils import setup_logger


# Tuple containing the source files and their relative paths
# This is not the ideal way to ingest these files
SOURCE_FILES = ("data/file1.csv", "data/file2.csv")

# Setup logging for the module
logger = setup_logger(__name__)


def main():
    # Step 1: Read the source files
    logger.info("Reading source files")
    source_data = read_source_files()

    # Create a list to hold cleaned and deduplicated dataframes
    finalized_dfs = []

    # For each source file, clean the dataframe and remove the duplicates
    for file in source_data:
        # Step 2: Clean the data
        logger.info(f"Cleaning {file} dataframe")
        clean_df = clean_data(source_data[file])

        # Step 3: Remove duplicates
        logger.info(f"Deduplicating {file} dataframe")
        deduped_df = remove_dupes(clean_df, file)

        finalized_dfs.append(deduped_df)

    # Step 4: Combine clean and deduplicated data into a single dataframe
    logger.info("Combining cleaned and deduplicated dataframes")
    final_df = combine_dataframes(finalized_dfs)

    # Step 5: Write the final dataframe to a CSV file
    logger.info("Writing final dataframe to CSV file")
    final_df.to_csv("data/output.csv", index=False)


def remove_dupes(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Remove duplicate rows from a Pandas DataFrame based on specified columns
    add an occurence count and source file indicators.

    This function identifies duplicate rows, adds a new column with the
    occurrence count, keeps the first occurrence of duplicates, and adds
    source file indicators to the DataFrame based on the 'source' argument.

    Args:
        df (pd.DataFrame): The input DataFrame to remove duplicates from.
        source (str): A string indicating the source of the data.

    Returns:
        pd.DataFrame: A DataFrame with duplicates removed, an occurence column
        and source file indicators.

    Raises:
        Any exceptions raised during DataFrame operations will be propagated
        to the caller.
    """

    # Get the count of duplicate rows in the dataframe
    dupes = df.duplicated().sum()
    logger.debug(f"Duplicate count: {dupes}")

    # Generate the occurence count based on name, address, city
    # and zip and add that as column to the dataframe
    logger.debug("Adding occurence count")
    df = df.groupby(["name", "address", "city", "zip"]
                    ).size().reset_index(name="occurrences").sort_values(["occurrences", "name"], ascending=[False, True])

    # Keep only the first occurence of duplicates based on the same grouping
    # criteria as the occurence count was generated
    logger.debug("Dropping duplicates")
    df = df.drop_duplicates(
        subset=["name", "address", "city", "zip"], keep="first")

    # Add the source file boolean columns based on the source string that was
    # passed into the function
    logger.debug("Adding source file columns")
    if source == "file1":
        df[source] = True
        df["file2"] = False
    else:
        df["file1"] = False
        df[source] = True

    return df


def combine_dataframes(dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Combine a list of cleaned and deduplicated Pandas DataFrames into a single
    dataframe sort it by 'name', and generate a unique ID for each row.

    This function takes a list of DataFrames, concatenates them into a single DataFrame,
    sorts the combined DataFrame by the 'name' column, and generates a unique identifier
    ('id') for each row based on the content of the row.

    Args:
        dataframes (list[pd.DataFrame]): A list of cleaned and deduplicated
        DataFrames to be combined.

    Returns:
        pd.DataFrame: A single combined DataFrame with a unique 'id' column.

    Raises:
        Any exceptions raised during DataFrame operations or unique ID generation
        will be propagated to the caller.
    """
    # Combine the list of cleaned and deduplicated dataframes into a single dataframe
    logger.debug("Combining dataframes")
    df = pd.concat(dataframes)

    # Sort the dataframe by name
    logger.debug("Sorting new dataframe by name")
    df = df.sort_values(by=["name"])

    # Create a unique ID for each row
    logger.debug("Generating unique IDs for each row")
    df["id"] = df.apply(lambda x: hashlib.md5(
        str(tuple(x)).encode('utf-8')).hexdigest(), axis=1)

    # Move the newly created id column to the first position
    logger.debug("Moving id column to first position")
    col = df.pop("id")
    df.insert(0, col.name, col)

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform data cleaning operations on a Pandas DataFrame and return
    the cleaned DataFrame.

    This function standardizes column names, fills missing values with defaults,
    converts string columns to lowercase, removes unnecessary whitespace, and
    applies various text transformations to prepare the data for analysis.

    Args:
        df (pd.DataFrame): The input DataFrame to be cleaned.

    Returns:
        pd.DataFrame: A cleaned DataFrame.

    Raises:
        Any exceptions raised during DataFrame operations
        will be propagated to the caller.
    """
    original_count = df.shape[0]
    logger.debug(f"Input DF count: {original_count}")

    # Standardize the column names based on position
    logger.debug("Renaming columns")
    df.rename(columns={df.columns[0]: "name", df.columns[1]: "address",
                       df.columns[2]: "city", df.columns[3]: "zip"}, inplace=True)

    # Fill any empty / na values
    logger.debug("Filling missing values")
    values = {"name": "not provided", "address": "not provided",
              "city": "not provided", "zip": 00000}
    df.fillna(value=values, inplace=True)

    # A list to specify which columns contain string datatypes
    str_cols = ["name", "address", "city"]

    # Convert all the string column values to lowercase
    logger.debug("Converting string to lowercase")
    df[str_cols] = df[str_cols].map(lambda x: x.lower())

    # Remove unnecessary whitespace
    logger.debug("Removing whitespace")
    df = df.map(lambda x: x.strip() if isinstance(
        x, str) else x)

    # Remove corporate suffixs from names
    logger.debug("Removing corporate suffixs")
    corp_pattern = "|".join([" llc", " inc", " ltd"])
    df["name"] = df["name"].str.replace(corp_pattern, "", regex=True)

    # Remove symbols and signs from string columns
    # This regex pattern matches any character that is not a word
    # or whitespace character
    logger.debug("Removing symbols and signs")
    pattern = r'[^\w\s]'
    df[str_cols] = df[str_cols].replace(pattern, "", regex=True)

    logger.debug(f"Cleaned DF count: {df.shape[0]}")

    # If the record count changes during cleaning something
    # went wrong no records should be removed
    if original_count != df.shape[0]:
        raise Exception("The dataframe record count has changed!")

    return df


def read_source_files() -> Dict[str, pd.DataFrame]:
    """
    Read source CSV files into dataframes and return a dictionary
    mapping file names to corresponding dataframes.

    This function iterates over a list of source CSV file paths,
    extracts the file name, and reads each file into a Pandas DataFrame.
    The resulting dataframes are stored in a dictionary where the keys
    are the file names and the values are the corresponding dataframes.

    Args:
        None

    Returns:
        Dict[str, pd.DataFrame]: A dictionary mapping file names to
        their respective dataframes.

    Raises:
        Any exceptions raised during file reading or DataFrame creation
        will be propagated to the caller.
    """
    # Create a dictionary to map the source file name to a dataframe
    source_details = {}

    # Read each source CSV file into a dataframe and add the file name
    # and dataframe to source_details
    for file in SOURCE_FILES:
        file_name = file.split("/")[1].split(".")[0]
        logger.debug(f"File name: {file_name}")
        source_details[file_name] = pd.read_csv(file, skipinitialspace=True)
        logger.debug(f"Source details: {source_details}")

    return source_details


if __name__ == "__main__":
    main()
