import mysql.connector
import os
from dotenv import load_dotenv
import pandas as pd

# .env 파일에서 환경 변수 로드
load_dotenv()

# 환경 변수 불러오기
mysql_host_peterpanz = os.getenv('MYSQL_HOST_PETERPANZ')
mysql_user_peterpanz = os.getenv('MYSQL_USER_PETERPANZ')
mysql_password_peterpanz = os.getenv('MYSQL_PASSWORD_PETERPANZ')
mysql_database_peterpanz = os.getenv('MYSQL_DATABASE_PETERPANZ')
mysql_database_dusedata = os.getenv('MYSQL_DATABASE_CAFE')
mysql_database_peterpanz_marketing = os.getenv('MYSQL_DATABASE_PETERPANZ_MARKETING')

mysql_host_marketing = os.getenv('MYSQL_HOST_MARKETING')
mysql_user_marketing = os.getenv('MYSQL_USER_MARKETING')
mysql_password_marketing = os.getenv('MYSQL_PASSWORD_MARKETING')
mysql_database_marketing = os.getenv('MYSQL_DATABASE_MARKETING')



def fetch_data_from_mysql(query, db_select, params=None):
    """
    MySQL 데이터베이스에서 데이터를 조회하여 Pandas DataFrame으로 반환하는 함수.

    Parameters:
    - query (str): 실행할 SQL 쿼리 (SQL 파일도 지원)
    - db_select (str): 사용할 데이터베이스 선택 ('peterpanz', 'cafe', 'marketing', 'peterpanz_marketing')
    - params (tuple): SQL 쿼리에 전달할 파라미터 값 (예: 날짜 범위)

    Returns:
    - df (pandas.DataFrame): 조회된 데이터를 포함하는 DataFrame
    """

    # MySQL 데이터베이스 연결
    if db_select == 'peterpanz':
        connection = mysql.connector.connect(
            host=mysql_host_peterpanz,
            user=mysql_user_peterpanz,
            password=mysql_password_peterpanz,
            database=mysql_database_peterpanz
        )

    elif db_select == 'cafe': 
        connection = mysql.connector.connect(
            host=mysql_host_peterpanz,
            user=mysql_user_peterpanz,
            password=mysql_password_peterpanz,
            database=mysql_database_dusedata
        )
                
    elif db_select == 'marketing':
        connection = mysql.connector.connect(
            host=mysql_host_marketing,
            user=mysql_user_marketing,
            password=mysql_password_marketing,
            database=mysql_database_marketing
        )
            
    elif db_select == 'peterpanz_marketing':
        connection = mysql.connector.connect(
            host=mysql_host_peterpanz,
            user=mysql_host_peterpanz,
            password=mysql_host_peterpanz,
            database=mysql_database_peterpanz_marketing
        )
            
    else:
        raise ValueError("유효하지 않은 데이터베이스 선택: 'peterpanz', 'cafe', 'marketing', 'peterpanz_marketing' 중 하나를 선택하세요.")
            
    # 커서 생성
    cursor = connection.cursor()

    try:
        # SQL 쿼리 실행 (파라미터 적용)
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # 조회된 데이터 가져오기
        results = cursor.fetchall()

        # 컬럼명 가져오기
        columns = [column[0] for column in cursor.description]

        # Pandas DataFrame으로 변환
        df = pd.DataFrame(results, columns=columns)

    finally:
        # 리소스 정리: 커서 및 데이터베이스 연결 닫기
        cursor.close()
        connection.close()

    return df