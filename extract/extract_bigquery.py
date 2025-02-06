from google.cloud import bigquery
import pandas as pd
import os
from google.oauth2 import service_account
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# 서비스 계정 키 파일 경로 가져오기
key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not key_path:
    raise FileNotFoundError("환경 변수 'GOOGLE_APPLICATION_CREDENTIALS'가 설정되지 않았습니다.")


# Credentials 객체 생성
default_credentials = service_account.Credentials.from_service_account_file(key_path)

def fetch_data_from_bigquery(query, credentials=default_credentials):
    """
    Google BigQuery에서 데이터를 조회하여 pandas DataFrame으로 반환하는 함수.

    Parameters:
    ----------
    query : str
        실행할 SQL 쿼리 (예: "SELECT * FROM dataset.table LIMIT 10").
    credentials : google.oauth2.service_account.Credentials, optional
        BigQuery 인증에 사용할 서비스 계정 객체 (기본값: default_credentials).

    Returns:
    -------
    pandas.DataFrame
        실행된 쿼리의 결과를 포함하는 DataFrame.

    Raises:
    ------
    google.api_core.exceptions.GoogleAPIError
        BigQuery API 요청 중 발생할 수 있는 오류.
    FileNotFoundError
        credentials 파일이 설정되지 않았거나 찾을 수 없는 경우 발생.

    Example:
    --------
    >>> query = "SELECT event_name FROM `project.dataset.events` LIMIT 100"
    >>> df = fetch_data_from_bigquery(query)
    >>> print(df.head())

    Notes:
    ------
    - 기본적으로 `GOOGLE_APPLICATION_CREDENTIALS` 환경 변수가 설정되어 있어야 BigQuery 인증이 가능함.
    - credentials를 직접 전달하지 않으면 `default_credentials`를 사용하여 인증함.
    """
    # GCP 클라이언트 객체 생성
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # 쿼리 실행
    query_job = client.query(query)

    # 결과를 pandas DataFrame으로 변환
    results = query_job.result()
    df = results.to_dataframe()

    return df
