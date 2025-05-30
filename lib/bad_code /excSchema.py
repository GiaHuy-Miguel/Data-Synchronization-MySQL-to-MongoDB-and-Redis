import mysql.connector

# # DB_CONFIG
# db_config = {
#     "host": "",
#     "port":,
#     "user": "",
#     "password": ""
# }

# CONNECT TO DB
connection = mysql.connector.connect(**db_config)
print('-------------------------connected to MySQL--------------------')
cursor = connection.cursor()


database_name = "GIT"
cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database_name}')
print(f'-------------------------database {database_name} created--------------------')

connection.database = database_name
print(f'-------------------------database {database_name} connected--------------------')

sql_file_path = "/Spark_DE/study/Problem-1-DataSynchronization/src/schema.sql"
with open(sql_file_path, "r") as schema:
    sql_script = schema.read()

sql_commands = sql_script.split(';')
# print(sql_commands)

for command in sql_commands:
    if command.strip():
        cursor.execute(command)
        print(f"-------------executed: {command.strip()[::50]}----------------------")

connection.commit()
print(f"-------------sql executed successfully----------------------")