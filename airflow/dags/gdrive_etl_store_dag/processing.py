import os
import re
import logging
from datetime import datetime
import numpy as np
import pandas as pd


def process_store_data(dataframes):
    """Process store data from multiple Excel sheets and combine them into a single DataFrame.
    This function processes store sales data from multiple Excel sheets, handling different
    formats of cashier information and combining them into a standardized DataFrame.
    Args:
        dataframes (dict): Dictionary of pandas DataFrames where keys are sheet names
            containing store numbers and values are the corresponding DataFrames.
    Returns:
        pandas.DataFrame: A processed and combined DataFrame with the following columns:
            - date (datetime): Date of the sales
            - revenue (float): Daily revenue
            - balance (float): End of day balance
            - seller_name (str): Name of the seller/cashier
            - store_id (int): Store identifier
    Notes:
        The function handles two different input formats:
        1. Sheets with "Фамилия, имя Кассира" column
        2. Sheets with "Фамилия Кассир №1" and "Фамилия Кассир №2" columns
        The function performs the following transformations:
        - Combines multi-level column headers
        - Extracts store numbers from sheet names
        - Converts dates to datetime format
        - Standardizes column names to English
        - Converts numeric values to appropriate types
        - Cleans and standardizes seller names
    """

    dfs = {}
    for key, df in dataframes.items():
        splitted = key.split("/")
        shop_name = splitted[-1] if len(splitted) > 1 else None
        cols_level_1 = list(df.iloc[0])
        cols_level_2 = list(df.iloc[1])
        combined_list = [
            cols_level_1[i] if not pd.isna(cols_level_1[i]) else cols_level_2[i]
            for i in range(len(cols_level_1))
        ]
        df.columns = combined_list
        df.drop([0, 1], inplace=True)

        if "Фамилия, имя Кассира" in combined_list:
            df = df[df["ОБЩАЯ ВЫРУЧКА (ПРИХОД)*"].notna()].copy()
            lst_of_cols = [
                "Дата",
                "ОБЩАЯ ВЫРУЧКА (ПРИХОД)*",
                "Фамилия, имя Кассира",
                "Остаток на конец дня*",
            ]
            df = df[lst_of_cols].copy()
            df.loc[:, "Магазин"] = shop_name
            df.rename(
                columns={
                    "ОБЩАЯ ВЫРУЧКА (ПРИХОД)*": "Выручка",
                    "Фамилия, имя Кассира": "ФИО",
                },
                inplace=True,
            )
            result = df.reset_index(drop=True)
            dfs[shop_name] = result
        elif "Фамилия Кассир №1" in combined_list:
            if "Фамилия Кассир №2 (ночь)" in combined_list:
                second_cashier = "Фамилия Кассир №2 (ночь)"
            else:
                second_cashier = "Фамилия Кассир №2"
            df = df[df["ОБЩАЯ ВЫРУЧКА (ПРИХОД)*"].notna()].copy()
            lst_of_cols_1 = [
                "Дата",
                "ОБЩАЯ ВЫРУЧКА (ПРИХОД)*",
                "Остаток на конец дня*",
                "Фамилия Кассир №1",
            ]
            lst_of_cols_2 = [
                "Дата",
                "ОБЩАЯ ВЫРУЧКА (ПРИХОД)*",
                "Остаток на конец дня*",
                second_cashier,
            ]
            df1 = df[lst_of_cols_1].copy()
            df2 = df[df[second_cashier].notnull()][lst_of_cols_2].copy()
            df1.rename(
                columns={
                    "ОБЩАЯ ВЫРУЧКА (ПРИХОД)*": "Выручка",
                    "Фамилия Кассир №1": "ФИО",
                },
                inplace=True,
            )
            df2.rename(
                columns={"ОБЩАЯ ВЫРУЧКА (ПРИХОД)*": "Выручка", second_cashier: "ФИО"},
                inplace=True,
            )
            result = pd.concat([df1, df2], ignore_index=True)
            result["Магазин"] = shop_name
            dfs[shop_name] = result

    concatenated_df = pd.concat(dfs.values(), ignore_index=True)

    # Use raw string for regex to avoid SyntaxWarning
    concatenated_df["Магазин"] = concatenated_df["Магазин"].str.extract(
        r"(\d+)", expand=False
    )

    # Handle NaNs before converting to int
    concatenated_df["Магазин"] = concatenated_df["Магазин"].fillna(0).astype(int)

    concatenated_df["Дата"] = pd.to_datetime(
        concatenated_df["Дата"], errors="coerce"
    ).dt.to_pydatetime()

    # Rename the columns
    concatenated_df.rename(
        columns={
            "Дата": "date",
            "Выручка": "revenue",
            "Остаток на конец дня*": "balance",
            "ФИО": "seller_name",
            "Магазин": "store_id",
        },
        inplace=True,
    )

    concatenated_df["balance"] = pd.to_numeric(
        concatenated_df["balance"], errors="coerce"
    )
    concatenated_df["revenue"] = pd.to_numeric(
        concatenated_df["revenue"], errors="coerce"
    )

    # Clean 'seller_name' column
    concatenated_df["seller_name"] = (
        concatenated_df["seller_name"].str.strip().fillna("Неизвестно")
    )

    return concatenated_df
