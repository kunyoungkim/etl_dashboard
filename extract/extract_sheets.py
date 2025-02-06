import gspread
from google.oauth2 import service_account
import os
from dotenv import load_dotenv
import pandas as pd

# 환경 변수 로드
load_dotenv()

# 서비스 계정 키 파일 경로 가져오기
key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not key_path:
    raise FileNotFoundError("환경 변수 'GOOGLE_APPLICATION_CREDENTIALS'가 설정되지 않았습니다.")

# `google-auth`를 사용하여 Credentials 객체 생성
default_credentials = service_account.Credentials.from_service_account_file(key_path, scopes=[
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
])

# ✅ gspread 클라이언트 생성 함수
def get_gspread_client(credentials=default_credentials):
    """Google Sheets API 인증을 위한 gspread 클라이언트를 생성하는 함수"""
    return gspread.authorize(credentials)

# ✅ Google Sheet 데이터를 불러오는 함수
def fetch_data_from_google_sheet(sheet_id, sheet_name, range_name, row_number=0):
    """
    Google Sheets에서 특정 시트(sheet_name)와 범위(range_name)의 데이터를 가져오는 함수.

    Parameters:
    - sheet_id (str): Google Sheets 문서의 ID
    - sheet_name (str): 불러올 시트의 이름
    - range_name (str): 데이터 범위 (예: "A1:C10")

    Returns:
    - list: Google Sheets에서 가져온 데이터 리스트
    """
    client = get_gspread_client()
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)


    # Google Sheets 데이터 가져오기
    data = sheet.get(range_name)

    # 빈 데이터 처리
    if not data:
        return pd.DataFrame()  # 데이터가 없을 경우 빈 DataFrame 반환

    # 첫 번째 행을 컬럼명으로 사용
    columns = data[row_number]
    rows = data[1:]  # 데이터 값

    # DataFrame 생성
    df = pd.DataFrame(rows, columns=columns)

    return df



# ✅ Sheet 여러 개 한꺼번에 불러오기 함수
def fetch_all_sheets(sheet_id, row_number, credentials=default_credentials):
    """
    Google Sheets 문서에서 모든 시트를 가져와 DataFrame으로 변환하는 함수.

    Parameters:
    - sheet_id (str): Google Sheets 문서의 ID
    - credentials (google.oauth2.service_account.Credentials, optional): 인증 정보 (기본값: default_credentials)

    Returns:
    - dict: {시트 이름: DataFrame} 형태의 딕셔너리
    """
    client = get_gspread_client(credentials)
    sheets = client.open_by_key(sheet_id)

    sheets_dict = {}  # ✅ dict → sheets_dict로 변경

    for worksheet in sheets.worksheets():
        sheet_name = worksheet.title

        # 열 이름을 가져옴 (A3:R3 범위의 열 이름 데이터)
        columns = worksheet.get_values(f'A{row_number}:{chr(64 + len(worksheet.row_values(2)))}3')[0]

        # 시트에서 데이터를 가져옴 (A4:R 범위 데이터)
        data = worksheet.get_values(f'A{row_number}:{chr(64 + len(worksheet.row_values(2)))}')

        # 값이 없을 경우 빈 문자열로 대체
        data = [[cell if cell else '' for cell in row] for row in data]

        # 데이터의 열 수가 부족하면 빈 데이터로 채움
        expected_columns = len(columns)
        for row in data:
            while len(row) < expected_columns:
                row.append('')

        # DataFrame 생성 및 딕셔너리에 추가
        sheets_dict[sheet_name] = pd.DataFrame(data, columns=columns, row_numb=3)

    return sheets_dict