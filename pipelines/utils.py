import pandas as pd
from typing import List


def contains_no_duplicates(data: pd.DataFrame):
    return not data.duplicated().any()


def contains_no_nulls(data: pd.DataFrame):
    return not data.isnull().any().any()


def contains_columns(data: pd.DataFrame, cols: List[str]):
    return not set(cols) - set(data.columns)


def is_less_than(data: pd.DataFrame, value: float):
    return (data < value).all().all()
