import pandas as pd

def fill_missing_values(dataframe, column, value):
    """특정 컬럼의 결측값을 채우는 함수"""
    dataframe[column] = dataframe[column].fillna(value)
    return dataframe

def convert_date_format(dataframe, column, old_format, new_format):
    """날짜 형식을 변환하는 함수"""
    dataframe[column] = dataframe[column].apply(
        lambda x: pd.to_datetime(x, format=old_format).strftime(new_format)
    )
    return dataframe

def calculate_activation_rate(data):
    """활성화율(Activation Rate)을 계산하는 함수"""
    data["activation_rate"] = data["activated_users"] / data["total_users"] * 100
    return data

def remove_duplicates(dataframe, subset_columns):
    """특정 컬럼을 기준으로 중복 제거"""
    return dataframe.drop_duplicates(subset=subset_columns)

def aggregate_by_date(dataframe, date_column, metrics):
    """날짜별로 데이터를 집계"""
    return dataframe.groupby(date_column)[metrics].sum().reset_index()

