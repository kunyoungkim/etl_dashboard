# 프로젝트 목적
AAARRR 프레임워크에 따라 전사 OMTM과 팀별 KPI를 설정하고, 이를 매일 모니터링하며 목표 대비 달성률을 체크합니다. 이를 기반으로 피드백을 반복해 지표를 개선합니다. 

# 프로젝트 상세 개요
- Period : 2024.04.29 ~ 2024.06.28 
- Programming Languages : Python(3.10.9), MySQL(8.0.31)
- Cloud : Google Cloud Platform
- Database : MySQL(8.0.31), BigQuery
- API : GA4, Appsflyer, Slack
- Web Framwork : Streamlit (ver.1.28.1)

# 주요 라이브러리 버전
- [requirement.txt](etl_dashboard/requirement.txt) 참조

# 파일 구조
```
etl_project/
├── extract/                   # 데이터 추출 모듈
│   ├── extract_ga4.py         # GA4 API로 데이터 추출
│   ├── extract_bigquery.py    # BigQuery에서 데이터 추출
│   ├── extract_sheets.py      # Google Sheets에서 데이터 추출
│   ├── extract_mysql.py       # MySQL에서 데이터 추출
│   └── extract_utils.py       # 공통 유틸리티
├── transform/                 # 데이터 변환 모듈
│   ├── transform_acquisition.py  # 획득 데이터 변환
│   ├── transform_activation.py   # 활성화 데이터 변환
│   ├── transform_retention.py    # 리텐션 데이터 변환
│   ├── transform_revenue.py      # 수익화 데이터 변환
│   ├── transform_refferal.py     # 추천 데이터 변환
│   └── transform_utils.py        # 공통 유틸리티
├── load/                      # 데이터 적재 모듈
│   ├── load_to_mysql.py       # 변환된 데이터를 MySQL에 적재
│   └── load_utils.py          # 적재 관련 공통 유틸리티
├── config/                    # 설정 파일
│   └── db_config.json         # MySQL 및 데이터베이스 연결 설정
└── main.py                    # 전체 ETL 파이프라인 실행
```

# 주요 기능
- 본 프로젝트에서 자체 개발 및 활용한 주요 메서드는 다음과 같습니다.

| Funtions | Location | Description |
| --- | --- | --- |
| format_report_with_pagination | extract_ga4.py | For converting GA4 data to DataFrame |
| calculate_date_range | extract_ga4.py | For calculating and return a date range |
| create_ga4_request | extract_ga4.py | For fetching GA4 data as a DataFrame |
| create_dimension_filter | extract_ga4.py | For creating a filter for dimensions |
| extract_ga4_data | extract_ga4.py | For fetching data from GA4 API |

# 함수 상세 설명
## 1. extract_ga4.py
- GA4 API를 활용하여 데이터를 불러오는 파일
- [GA4 API 사용법에 대한 자세히 정리한 문서](https://github.com/kunyoungkim/ga4-api?tab=readme-ov-file) 
## 1-1. create_ga4_request
- 이 함수는 GA4 탐색 보고서처럼 원하는 형태로 집계된 GA4 데이터를 추출하기 위한 함수입니다.
- 측정기준, 측정항목, 날짜 범위, 측정기준 필터를 설정하여 데이터를 집계할 수 있습니다.
- 불러올 수 있는 측정기준, 측정항목의 목록은 [이 문서](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema?hl=ko)를 참고해주세요. 
- 측정기준에서 '날짜'는 기본적으로 불러올 수 있게 설정되었습니다. 이 때, default_dimension을 'yearMonth'로 바꿔서 '년월' 기준으로 집계를 할 수 있습니다.
- 사용 예시
```
create_ga4_request('firstUserSourceMedium', #newVsReturning
                    ['totalUsers', 'activeUsers', 'newUsers', 'eventCount', 'eventCountPerUser'],
                    start=start)
```

## 1-2. create_dimension_filter
- 이 함수는 create_ga4_request 함수를 통해 데이터를 불러올 때, 측정기준에 대한 필터를 걸 수 있는 함수 입니다.
- 사용 예시
```
install_users = create_ga4_request('firstUserSourceMedium', #newVsReturning
            ['activeUsers', 'eventCount', 'eventCountPerUser'],
            start=start,
            dimension_filter=create_dimension_filter('eventName', 'install', match_type1=Filter.StringFilter.MatchType.CONTAINS)
            )
```

# Release Notes

# License

