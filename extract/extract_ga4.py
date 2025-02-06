from datetime import date, timedelta, datetime
import pandas as pd
from typing import Union, List, Optional
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import os

# GA4관련 라이브러리
from google.analytics.data_v1beta.types import CohortSpec, CohortsRange, Cohort
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    Dimension,
    Metric,
    OrderBy,
    DateRange,
    FilterExpression,
    Filter,
    FilterExpressionList
)
from google.analytics.data_v1beta.types import Filter

# .env 파일 로드
load_dotenv()

property_id = os.getenv('GA_PROPERTY_ID')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

def format_report_with_pagination(request, row_limit=100000, page_size=1000):
    """
    #GA4 응답 데이터를 DataFrame으로 변환하는 함수 (페이징 포함)
    """
    client = BetaAnalyticsDataClient()
    all_data = []
    offset = 0
    
    while offset < row_limit:
        # 각 페이지에 대해 limit와 offset을 설정하여 요청
        request.limit = page_size
        request.offset = offset
        
        # GA4 데이터 요청
        response = client.run_report(request)
        
        # 행 데이터를 추출하여 리스트에 추가
        for row in response.rows:
            row_data = {dim.name: row.dimension_values[i].value for i, dim in enumerate(response.dimension_headers)}
            row_data.update({metric.name: float(row.metric_values[i].value) for i, metric in enumerate(response.metric_headers)})

            # 날짜 형식 변환 (20240827 -> 2024-08-27)
            if 'date' in row_data:
                original_date = row_data['date']
                formatted_date = f"{original_date[:4]}-{original_date[4:6]}-{original_date[6:]}"
                row_data['date'] = formatted_date

            all_data.append(row_data)
        
        # 다음 페이지로 이동
        offset += page_size
        
        # 모든 데이터가 다 불러와졌으면 중지
        if len(response.rows) < page_size:
            break
    
    return pd.DataFrame(all_data)


def calculate_date_range(default_dimension: str, start: int = None) -> List[DateRange]:
    """
    날짜 범위를 계산하여 반환. default_dimension에 따라 다른 방식으로 계산.
    """
    today = date.today()

    if default_dimension == 'date':
        return [DateRange(
            start_date=(today - timedelta(days=start)).strftime('%Y-%m-%d'),
            end_date=today.strftime('%Y-%m-%d')
        )]
    elif default_dimension == 'yearMonth':
        return [DateRange(
            start_date=f'{today.replace(day=1).strftime("%Y-%m-%d")}',
            end_date=today.strftime('%Y-%m-%d')
        )]
    return []


def create_ga4_request(
    dimensions: Union[str, List[str]] = None,
    metrics: Union[str, List[str]] = None,
    start: int = None,
    dimension_filter: FilterExpression = None,
    default_dimension: str = 'date'
) -> pd.DataFrame:
    """
    GA4 데이터를 요청하고 Pandas DataFrame으로 반환하는 함수.

    측정기준(dimensions), 측정항목(metrics), 필터 조건 등을 지정하여 GA4 데이터 보고서를 생성하며,
    기본적으로 날짜별(default_dimension='date') 데이터를 반환합니다.

    Args:
    dimensions (Union[str, List[str]]): GA4에서 가져올 측정기준 (예: ['date', 'platform']).
                                        단일 값(str) 또는 리스트(List)로 입력 가능.
    metrics (Union[str, List[str]]): GA4에서 가져올 측정항목 (예: ['activeUsers', 'eventCount']).
                                     단일 값(str) 또는 리스트(List)로 입력 가능.
    start (int): 데이터를 조회할 날짜 범위의 시작점 (예: 최근 30일 데이터를 가져오려면 `start=30`).
                 오늘을 기준으로 몇 일 전부터 데이터를 가져올지 지정.
    dimension_filter (FilterExpression): GA4에서 데이터를 필터링할 조건.
                                          `create_dimension_filter`를 통해 생성된 필터를 사용.
                                          필터가 없으면 전체 데이터가 반환됨.
    default_dimension (str): 기본 측정기준 (예: 'date', 'yearMonth').
                             기본값은 'date'이며, 데이터를 날짜별로 집계.

    Returns:
    pd.DataFrame: GA4에서 반환된 데이터를 Pandas DataFrame으로 변환한 결과.
                  각 행은 요청된 측정기준과 측정항목에 해당하는 데이터를 포함.

    Example Usage:
    --------------
    # 필터 생성
    dimension_filter = create_dimension_filter(
        field1='platform', 
        values1=['iOS', 'Android'], 
        field2='unifiedScreenClass', 
        values2=['HomeVC', 'MainVC']
    )
    
    # 데이터 요청
    df = create_ga4_request(
        dimensions=['date', 'platform', 'unifiedScreenClass'],
        metrics=['activeUsers', 'eventCount'],
        start=30,
        dimension_filter=dimension_filter
    )
    
    # 결과 출력
    print(df.head())
    """
    global property_id  # 전역 변수 사용 선언

    # dimensions와 metrics 처리
    if isinstance(dimensions, str):
        dimensions = [dimensions]
    if isinstance(metrics, str):
        metrics = [metrics]
    dimensions = dimensions or [default_dimension]
    metrics = metrics or []

    # 기본 dimension 추가
    if default_dimension not in dimensions:
        dimensions.insert(0, default_dimension)

    # Dimension, Metric 객체 생성
    dimension_objects = [Dimension(name=dim) for dim in dimensions]
    metric_objects = [Metric(name=met) for met in metrics]

    # 날짜 범위 계산
    date_ranges = calculate_date_range(default_dimension, start)

    # GA4 요청 생성
    request = RunReportRequest(
        property=f'properties/{property_id}',
        dimensions=dimension_objects,
        metrics=metric_objects,
        order_bys=[OrderBy(dimension={'dimension_name': default_dimension})],
        date_ranges=date_ranges,
        dimension_filter=dimension_filter
    )

    return format_report_with_pagination(request)


from typing import List, Optional, Union
from google.analytics.data_v1beta.types import Filter, FilterExpression, FilterExpressionList

def create_dimension_filter(
    field1: str, 
    values1: str, 
    field2: Optional[str] = None,  
    values2: Optional[Union[str, str]] = None,  
    match_type1: Filter.StringFilter.MatchType = Filter.StringFilter.MatchType.EXACT, 
    match_type2: Filter.StringFilter.MatchType = Filter.StringFilter.MatchType.EXACT,
    exclude1: bool = False,
    exclude2: bool = False
) -> FilterExpression:
    """
    GA4 측정기준 필터를 동적으로 생성하는 함수.

    Args:
    field1 (str): 필터링할 첫 번째 필드
    values1 (List[str]): 첫 번째 필드의 필터 값 목록
    field2 (str, optional): 필터링할 두 번째 필드 (기본값: None)
    values2 (Union[str, List[str]], optional): 두 번째 필드의 필터 값 목록 (기본값: None)
    match_type1 (Filter.StringFilter.MatchType): 첫 번째 필드의 매치 타입 (기본값: EXACT)
    match_type2 (Filter.StringFilter.MatchType): 두 번째 필드의 매치 타입 (기본값: EXACT)
    exclude1 (bool): 첫 번째 필터를 제외할지 여부 (기본값: False)
    exclude2 (bool): 두 번째 필터를 제외할지 여부 (기본값: False)

    Returns:
    FilterExpression: GA4 요청에 사용할 필터 표현식
    """
    
    # values2가 None이면 빈 리스트로 변환
    if values2 is None:
        values2 = []

    # # 단일 문자열을 리스트로 변환
    # if isinstance(values1, str):
    #     values1 = [values1]
    # if isinstance(values2, str):
    #     values2 = [values2]

    # 필터1 생성
    filter1_expression = FilterExpression(
        or_group=FilterExpressionList(
            expressions=[
                FilterExpression(
                    filter=Filter(
                        field_name=field1,
                        string_filter=Filter.StringFilter(value=values1, match_type=match_type1)
                    )
                )
            ]
        )
    )

    # not 조건 필터1 생성
    filter1_not_expression = FilterExpression(
        not_expression =
            FilterExpression(
                filter=Filter(
                    field_name=field1,
                    string_filter=Filter.StringFilter(value=values1, match_type=match_type1)
                )
            )
        )

    # 필터1 적용
    filter1 = filter1_not_expression if exclude1 else filter1_expression

    # 필터2 생성 (필드가 존재하고 값이 있을 경우만)
    if field2 and values2:
        filter2_expression = FilterExpression(
            or_group=FilterExpressionList(
                expressions=[
                    FilterExpression(
                        filter=Filter(
                            field_name=field2,
                            string_filter=Filter.StringFilter(value=value, match_type=match_type2)
                        )
                    ) for value in values2
                ]
            )
        )

        # not 조건 필터2 생성
        filter2_not_expression = FilterExpression(
            not_expression=FilterExpression(
                or_group=FilterExpressionList(
                    expressions=[
                        FilterExpression(
                            filter=Filter(
                                field_name=field2,
                                string_filter=Filter.StringFilter(value=value, match_type=match_type2)
                            )
                        ) for value in values2
                    ]
                )
            )
        )

        filter2 = filter2_not_expression if exclude2 else filter2_expression

        # 필터1과 필터2 결합
        combined_filter = FilterExpression(
            and_group=FilterExpressionList(
                expressions=[filter1, filter2]
            )
        )
    else:
        combined_filter = filter1

    return combined_filter


# 리텐션 데이터
def retention(platform: bool = False, date_format: str = 'day', end_offset: int = 1, before_month: int = 12):
    """
    Google Analytics 4 (GA4) 코호트 유지율 분xw석을 위한 함수.
    
    특정 기간 동안의 사용자 유지율을 일/주/월 단위로 분석할 수 있으며, 
    플랫폼(디바이스)별 분석도 가능하다.

    Parameters:
    -----------
    platform : bool, optional
        True일 경우, 플랫폼(디바이스 카테고리)별로 데이터를 분석 (기본값: False)
    
    date_format : str, optional
        유지율 분석 단위 설정 ('day', 'week', 'month' 중 선택, 기본값: 'day')
    
    end_offset : int, optional
        유지율 분석 기간 설정 (예: 14일, 12주, 12개월 등, 기본값: 14)
    
    before_month : int, optional
        분석 시작일 기준으로 몇 개월 전부터 데이터를 가져올지 설정 (기본값: 12개월)

    Returns:
    --------
    pandas.DataFrame or dict of pandas.DataFrame
        platform=False인 경우, 전체 데이터를 포함한 DataFrame을 반환.
        platform=True인 경우, 각 디바이스별 DataFrame을 포함하는 딕셔너리를 반환.

    Available Device Categories (platform=True):
    --------------------------------------------
    dict_keys([
        'web / mobile', 'web / desktop', 'iOS / mobile', 'Android / mobile', 
        'web / tablet', 'iOS / tablet', 'Android / tablet', 
        'iOS / desktop', 'web / smart tv'
    ])

    Example:
    --------
    >>> df = retention(platform=False, date_format='month', end_offset=12, before_month=6)
    >>> df.head()
    
    >>> device_dfs = retention(platform=True, date_format='week', end_offset=8, before_month=3)
    >>> print(device_dfs.keys())  # 디바이스별 데이터 확인
    """

    # Google Analytics 4 속성 ID 설정
    global property_id

    # GA4 클라이언트 생성
    client = BetaAnalyticsDataClient()
    today = date.today()

    if date_format == 'day':
        freq = 'D'
        cohort_demention = 'cohortNthDay'
        granularity = 'DAILY'
        col_name = 'Day'
        dates = pd.date_range(f'{today - relativedelta(months=before_month)}', today, freq=freq)
        cohort_labels = [date.strftime('%Y-%m-%d') for date in dates]
        end_dates = [(date).strftime("%Y-%m-%d") for date in dates]
        start_dates = [(date).strftime("%Y-%m-%d") for date in dates]
    elif date_format == 'week':
        freq = 'W-MON'  # 매주 월요일 기준
        cohort_demention = 'cohortNthWeek'
        granularity = 'WEEKLY'
        col_name = 'Week'
        dates = pd.date_range(today - relativedelta(months=before_month), today, freq=freq)
        cohort_labels = [date.strftime('%Y-%m-%d') for date in dates]  # 각 주의 월요일
        start_dates = [date.strftime('%Y-%m-%d') for date in dates]
        # 종료일: 같은 주의 일요일 (월요일 + 6일)
        end_dates = [(date + timedelta(days=6)).strftime('%Y-%m-%d') for date in dates]
    elif date_format == 'month':
        freq = 'MS'
        cohort_demention = 'cohortNthMonth'
        granularity = 'MONTHLY'
        col_name = 'Month'
        # 월 단위로 날짜 범위 설정
        first_day_of_current_month = today.replace(day=1)
        dates = pd.date_range(first_day_of_current_month - relativedelta(months=before_month), first_day_of_current_month, freq=freq)
        cohort_labels = [date.strftime('%Y-%m') for date in dates]
        start_dates = [(date - relativedelta(months=1)).strftime("%Y-%m-%d") for date in dates]
        end_dates = [(date - timedelta(days=1)).strftime("%Y-%m-%d") for date in dates]

    # Cohort 객체 생성
    cohorts = [
        Cohort(
            name=label,
            dimension='firstSessionDate',
            date_range=DateRange(start_date=start, end_date=end)
        )
        for label, start, end in zip(cohort_labels, start_dates, end_dates)
    ]

    # 요청 생성 (platform 여부에 따라 다르게 설정)
    dimensions = [
        Dimension(name="cohort"),
        Dimension(name=cohort_demention)
    ]
    
    if platform:
        dimensions.insert(0, Dimension(name="platformDeviceCategory"))

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=dimensions,
        metrics=[
            Metric(name="cohortActiveUsers")
        ],
        cohort_spec=CohortSpec(
            cohorts=cohorts,
            cohorts_range=CohortsRange(granularity=granularity, end_offset=end_offset),
        )
    )

    # 요청 실행
    response = client.run_report(request)

    # DataFrame 준비
    columns = ['cohort_date'] + [f'{col_name} {n}' for n in range(end_offset + 1)]

    if platform:
        device_category_dfs = {}

        for row in response.rows:
            platform_device_category = row.dimension_values[0].value
            cohort_date = row.dimension_values[1].value  # YYYY-MM 형식으로 변환
            date_index = int(row.dimension_values[2].value)
            active_users = int(row.metric_values[0].value)

            if platform_device_category not in device_category_dfs:
                device_category_dfs[platform_device_category] = {col: [] for col in columns}

            data = device_category_dfs[platform_device_category]

            if cohort_date not in data['cohort_date']:
                data['cohort_date'].append(cohort_date)
                for col in columns[1:]:
                    data[col].append(0)

            data[f'{col_name} {date_index}'][data['cohort_date'].index(cohort_date)] = active_users

        # 각 디바이스 카테고리별로 DataFrame 생성 및 정리
        for category, data in device_category_dfs.items():
            df = pd.DataFrame(data).set_index('cohort_date').sort_index()
            device_category_dfs[category] = df
        return device_category_dfs

    else:
        # 데이터 저장을 위한 딕셔너리 초기화
        data = {}

        for row in response.rows:
            cohort_date = row.dimension_values[0].value  # YYYY-MM 형식으로 변환
            date_index = int(row.dimension_values[1].value)
            active_users = int(row.metric_values[0].value)

            if cohort_date not in data:
                data[cohort_date] = {}

            data[cohort_date][date_index] = active_users

        # 데이터프레임 변환
        df = pd.DataFrame.from_dict(data, orient='index').fillna(0)

        # 컬럼명 정리 (Month 0, Month 1, ..., Month 12)
        df.columns = [f"{col_name} {col}" for col in df.columns]
        df.index.name = "cohort_date"

        # 컬럼 이름을 정렬
        sorted_columns = sorted(df.columns, key=lambda x: int(x.split(" ")[1]))

        # 정렬된 컬럼 순서로 데이터프레임 재정렬, 인덱스 내림차순으로 정렬
        df = df[sorted_columns].sort_index()

        return df
