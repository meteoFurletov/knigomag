import re
import logging
import numpy as np
import pandas as pd


def process_sales_data(file_path):
    """ETL "Выручка" data from 1c system

    Args:
        file_path (str): path to data

    Returns:
        df (pd.DataFrame): result in df
    """
    logging.info(f"Processing shop date data from file: {file_path}")

    # Load the data from the downloaded files
    df = pd.read_excel(file_path, engine="openpyxl", skiprows=8)

    # Drop rows with all NaN values
    df.dropna(axis=1, how="all", inplace=True)

    # Assign columns names
    df.columns = ["position", "quantity", "price"]

    # Remove trailing rows that contain only NaNs
    df = df.iloc[:-1]

    # Initialize shop and date columns as string types to avoid setting item incompatibility
    df["store_id"] = ""
    df["date"] = ""

    # Vectorized operations to identify shops and dates
    shop_pattern = re.compile(
        r"М-\d|М\d{1,2}\s|Мандарин \(павильон\)|Основной склад|Озон|О Вайлберис"
    )

    date_pattern = re.compile(r"\d{2}\.\d{2}\.\d{4}")
    shops = df["position"].apply(lambda x: bool(shop_pattern.match(str(x))))
    dates = df["position"].apply(lambda x: bool(date_pattern.match(str(x))))
    df.loc[shops, "store_id"] = df["position"]
    df.loc[dates, "date"] = df["position"]

    # Forward fill shop and date columns
    df["store_id"] = df["store_id"].replace("", np.nan).ffill()
    df["date"] = df["date"].replace("", np.nan).ffill()

    # Filter out the rows containing shop names and dates
    df = df[~(shops | dates)]

    # Reset index
    df.reset_index(drop=True, inplace=True)

    # Convert numeric columns
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Convert date column to datetime
    df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")

    return df
