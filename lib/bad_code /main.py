import mysql.connector
from mysql.connector import Error
from config.database_config import get_dbconfig
from pathlib import Path

DATABASE_NAME = "GIT"
SQL_FILE_PATH = Path("/bad_code /src/mysql_schema.sql")

def connect_to_mysql(config):
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except ConnectionError as e:
        raise Exception(f"-------------------cannot connect to MySQL: {e}--------------------") from e

def create_database(cursor, db_name):
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
    print(f'-------------------------database_connect{db_name} created--------------------')

def create_table(cursor, file_path):
    with open(file_path, "r") as file:
        sql_script = file.read()

    commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip() ]

    for cmd in commands:
        try:
            cursor.execute(cmd)
            print(f"-------------executed: {cmd.strip()[::50]}----------------------")
        except Error as e:
            print(f"--------------------cannot execute SQL: {e}-------------------")

def main():
    try:
        # Get db_config without db
        db_config = get_dbconfig()
        initial_config = {k:v for k,v in db_config.items() if k != "database"}
        # print(initial_config)

        # Connect to MySQL
        connection = connect_to_mysql(initial_config)
        cursor = connection.cursor()

        # Create DB
        create_database(cursor, db_name="GIT")
        connection.database = "GIT"

        # Create Table
        create_table(cursor, SQL_FILE_PATH)

        # Commit changes to DB
        connection.commit()
    except Exception as e:
        print(f"------------------Error: {e}------------------")
        if connection and connection.is_connected():
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
        print("----------------Disconnect from MySQL--------------------")
if __name__ == "__main__":
    main()