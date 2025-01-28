from datetime import date, timedelta, datetime
import pandas as pd
from typing import Union, List, Optional
import os

# GA4관련 라이브러리
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

property_id = os.getenv('PROPERTY_ID')
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
    property_id = property_id

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

# GA4 필터 함수
def create_dimension_filter(field1: str, 
                           values1: List[str], 
                           field2: Optional[str] = None,  # Optional parameter
                           values2: Optional[Union[str, List[str]]] = None,  # Optional parameter
                           match_type1: Filter.StringFilter.MatchType = Filter.StringFilter.MatchType.EXACT, 
                           match_type2: Filter.StringFilter.MatchType = Filter.StringFilter.MatchType.EXACT) -> FilterExpression:
    """
    플랫폼 및 화면 클래스를 기반으로 필터를 생성하는 함수. 필드 이름과 매치 타입도 동적으로 지정 가능.
    platform_match_type과 screen_class_match_type의 기본값은 'EXACT'.

    Args:
    field1 (str): 측정기준 이름 (예: 'platform')
    field2 (str): 측정기준 이름 (예: 'unifiedScreenClass')
    values1 (List[str]): 필터링할 측정기준 값 목록 (예: ['iOS', 'Android'])
    values2 (List[str]): 필터링할 측정기준 값 목록 (예: ['HomeVC', 'MainVC'])
    match_type1 (Filter.StringFilter.MatchType): 플랫폼 필터의 매치 타입 (기본값: EXACT)
    match_type2 (Filter.StringFilter.MatchType): 화면 클래스 필터의 매치 타입 (기본값: EXACT)

    Returns:
    FilterExpression: GA4 요청에 사용할 필터 표현식
    """
    
    # 단일 문자열인 경우 리스트로 변환
    if isinstance(values1, str):
        values1 = [values1]

    # values2가 제공되지 않으면 None으로 설정
    if values2 is None:
        values2 = []

    if isinstance(values2, str):
        values2 = [values2]

    # 필터1 생성
    filter1 = FilterExpression(
        or_group=FilterExpressionList(
            expressions=[
                FilterExpression(
                    filter=Filter(
                        field_name=field1,
                        string_filter=Filter.StringFilter(value=value, match_type=match_type1)
                    )
                ) for value in values1
            ]
        )
    )

    # 두 번째 필터가 있는 경우에만 필터2 생성
    if field2 and values2:
        filter2 = FilterExpression(
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
        # 두 필터를 결합
        combined_filter = FilterExpression(
            and_group=FilterExpressionList(
                expressions=[
                    filter1,
                    filter2
                ]
            )
        )
    else:
        # 첫 번째 필터만 사용할 경우
        combined_filter = filter1

    return combined_filter
