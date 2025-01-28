# load_to_mysql.py
import pymysql

def load_to_mysql(data, table_name, db_config):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    
    for row in data:
        sql = f"INSERT INTO {table_name} (date, source, new_users, activation_rate) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (row['date'], row['source'], row['new_users'], row['activation_rate']))
    
    connection.commit()
    connection.close()

