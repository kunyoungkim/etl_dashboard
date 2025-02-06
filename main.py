import json
import os
from extract.extract_ga4 import create_ga4_request, create_dimension_filter
from concurrent.futures import ThreadPoolExecutor
from extract.extract_mysql import fetch_data_from_mysql  # ✅ MySQL 데이터 조회 함수 불러오기
from datetime import datetime, timedelta


# 🔹 JSON 파일 로드 함수
def load_json_config(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
        default_start = data["default_start"]  # 공통 start 값
        configs = data["data_configs"]

        # 모든 설정에 동일한 start 값 적용
        for config in configs:
            config["start"] = default_start

        return [{k: v for k, v in config.items() if k != "comment"} for config in configs]  # "comment" 필드 제거
    
# 🔹 설정 파일 경로
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config", "etl_config.json")
DATA_CONFIGS = load_json_config(CONFIG_PATH) # default_start 바꿔서 사용할려면: (CONFIG_PATH, default_start=45)

# 🔹 ETL 처리 함수
def etl_process(config):
    print(f"🔄 {config['dimensions']} 데이터 처리 중...")

    # 1️⃣ 데이터 추출 (Extract)
    dimension_filter = create_dimension_filter(**config["filters"])
    raw_data = create_ga4_request(
        dimensions=config["dimensions"],
        metrics=config["metrics"],
        start=config["start"],
        dimension_filter=dimension_filter
    )

    # # 2️⃣ 데이터 변환 (Transform) - 필요하면 변환 적용
    # transformed_data = process_data(raw_data) if "transform" in config else raw_data

    # # 3️⃣ 데이터 적재 (Load)
    # table_name = "_".join(config["dimensions"])  # 예: 'date_platform_unifiedScreenClass'
    # load_to_mysql(transformed_data, table_name, DB_CONFIG)

    # print(f"✅ {table_name} 데이터 저장 완료!")


# 🔹 병렬 처리 (멀티스레딩)
if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(etl_process, DATA_CONFIGS)

    print("🚀 ETL 파이프라인 완료!")
