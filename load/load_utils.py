import pymysql

def insert_into_mysql(data, table_name, db_config):
    """MySQL 테이블에 데이터를 삽입하는 함수"""
    if not data:
        print(f"No data to insert into {table_name}")
        return
    
    columns = ", ".join(data[0].keys())
    values_template = ", ".join(["%s"] * len(data[0]))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_template})"
    
    connection = pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"]
    )
    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, [tuple(row.values()) for row in data])
        connection.commit()
    finally:
        connection.close()

def update_mysql(data, table_name, update_columns, where_column, db_config):
    """MySQL 테이블에서 데이터를 업데이트하는 함수"""
    set_clause = ", ".join([f"{col} = %s" for col in update_columns])
    query = f"UPDATE {table_name} SET {set_clause} WHERE {where_column} = %s"
    
    connection = pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"]
    )
    try:
        with connection.cursor() as cursor:
            for row in data:
                update_values = [row[col] for col in update_columns] + [row[where_column]]
                cursor.execute(query, update_values)
        connection.commit()
    finally:
        connection.close()

def truncate_table(table_name, db_config):
    """MySQL 테이블 초기화 (모든 데이터 삭제)"""
    connection = pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"]
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
        connection.commit()
    finally:
        connection.close()

