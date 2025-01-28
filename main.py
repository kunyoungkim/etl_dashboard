# -*- coding:utf-8 -*- 
from extract.extract_ga4 import extract_ga4_data
from transform.transform_activation import calculate_activation_rate
from load.load_to_mysql import load_to_mysql

if __name__ == "__main__":
    # Step 1: 데이터 추출
    raw_data = extract_ga4_data(property_id="123456789", start_date="2025-01-01", end_date="2025-01-31")
    
    # Step 2: 데이터 변환
    transformed_data = calculate_activation_rate(raw_data)
    
    # Step 3: 데이터 적재
    db_config = {"host": "localhost", "user": "root", "password": "password", "database": "aarr_metrics"}
    load_to_mysql(transformed_data, "activation_daily", db_config)
