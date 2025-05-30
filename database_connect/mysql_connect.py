import mysql.connector

# from config.database_config import get_dbconfig
from pathlib import Path

from mysql.connector.abstracts import MySQLCursorAbstract, MySQLConnectionAbstract


class MySQLConnect:
# DATABASE_NAME = "GIT"
# SQL_FILE_PATH = Path("/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/database_connect/mysql_schema.sql")
    def __init__(self, host, port, password, user):
        self.config = {"host": host, "port": port, "user": user, "password": password}
        self.cursor = None
        self.connection = None

    def connect(self):
        try:
            connection = mysql.connector.connect(**self.config)
            cursor = connection.cursor()
            return connection, cursor
        except Exception as e:
            print(f"------------------Error: {e}------------------")
            return None

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
        print("----------------Disconnect from MySQL--------------------")

    def __enter__(self):
        self.connect()
        print("-------------------------Connected to MySQL------------------------")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return self


