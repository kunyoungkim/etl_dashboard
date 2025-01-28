import gspread
from oauth2client.service_account import ServiceAccountCredentials

def fetch_data_from_google_sheet(sheet_id, range_name, credentials_json):
    # 구글 시트 API 인증
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets.readonly", "https://www.googleapis.com/auth/drive.readonly"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_json, scope)
    client = gspread.authorize(creds)

    # Google Sheet 접근
    sheet = client.open_by_key(sheet_id).worksheet('Sheet1')  # 'Sheet1'은 시트 이름, 필요에 따라 변경

    # 데이터 가져오기
    data = sheet.get(range_name)  # range_name은 예: "A1:C10"

    return data

