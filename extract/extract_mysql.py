import mysql.connector
import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

# 환경 변수 불러오기
mysql_host = os.getenv('MYSQL_HOST')
mysql_user = os.getenv('MYSQL_USER')
mysql_password = os.getenv('MYSQL_PASSWORD')
mysql_database = os.getenv('MYSQL_DATABASE')

def fetch_data_from_mysql(query):
    # MySQL 데이터베이스 연결
    connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )

    # 커서 생성
    cursor = connection.cursor()

    # 쿼리 실행
    cursor.execute(query)

    # 결과 가져오기
    results = cursor.fetchall()

    # 컬럼 이름을 가져오기
    columns = [column[0] for column in cursor.description]

    # pandas DataFrame으로 변환
    df = pd.DataFrame(results, columns=columns)

    # 연결 종료
    cursor.close()
    connection.close()

    return df
