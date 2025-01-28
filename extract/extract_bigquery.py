from google.cloud import bigquery
import pandas as pd

project_id = os.getenv('PROJECT_ID')

def fetch_data_from_bigquery(project_id, query):
    # BigQuery 클라이언트 초기화
    client = bigquery.Client(project=project_id)

    # 쿼리 실행
    query_job = client.query(query)

    # 결과를 pandas DataFrame으로 가져오기
    results = query_job.result()
    df = results.to_dataframe()

    return df

