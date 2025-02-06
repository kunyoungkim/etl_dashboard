from extract_ga4 import create_ga4_request, create_dimension_filter, retention


# import fetch_data_from_bigquery from extract_bigquery
# import fetch_data_from_google_sheet from extract_sheets
# import fetch_data_from_mysql from extract_mysql


# print("Creating dimension filter...")
# dimension_filter = create_dimension_filter(
#     field1='platform', 
#     values1=['iOS', 'Android'], 
#     field2='unifiedScreenClass', 
#     values2=['HomeVC', 'MainVC']
# )
# print("Dimension filter created:", dimension_filter)

# print("Sending request to GA4 API...")
# df = create_ga4_request(
#     dimensions=['date', 'platform', 'unifiedScreenClass'],
#     metrics=['activeUsers', 'eventCount'],
#     start=30,
#     dimension_filter=dimension_filter
# )
# print("Data received:", df.head())


# def extract_from_ga4():

#     web_au_meida_source = create_ga4_request(
#         dimensions=['sessionSourceMediums'],
#         metrics='eventCount',
#         start=30,
#         dimension_filter=dimension_filter
#     )

#     return