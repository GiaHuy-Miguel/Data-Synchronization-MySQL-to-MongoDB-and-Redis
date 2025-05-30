# IMPORT FOR MYSQL
from mysql.connector import Error

# IMPORT FOR MONGODB
from collections.abc import Mapping
from bson.int64 import Int64
from pymongo.synchronous.database import Database

#
# ----------------------------------- FOR MYSQL ------------------------------------------
#
def create_mysql_schema(cursor, db_name, file_path):
    # CREATE DATABASES
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
    print(f'-------------------------database_connect{db_name} created--------------------')

    #CREATE TABLES
    with open(file_path, "r") as file:
        sql_script = file.read()
    commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
    for cmd in commands:
        try:
            cursor.execute(cmd)
            print(f"-------------executed: {cmd.strip()[::50]}----------------------")
        except Error as e:
            print(f"--------------------cannot execute SQL: {e}-------------------")

def validate_mysql_schema(cursor):
    cursor.execute("SHOW TABLES;")
    tables = [row[0] for row in cursor.fetchall()]
    if "Users" not in tables or "Repo" not in tables:
        raise ValueError("-----------------------------Missing table-------------------------")

    cursor.execute("SELECT * FROM Users WHERE users_id = 1")
    user = cursor.fetchone()
    print(user)
    if not user:
        raise ValueError("-----------------------------Missing record-------------------------")
    print("--------------------------MySQL DB Schema Validated----------------------")
    cursor.execute("DELETE FROM Users WHERE users_id = 1")

#
# ----------------------------------- FOR MONGODB ------------------------------------------
#
def create_mongo_schema(collection_name:str, db: Database[Mapping[str]]):
    if collection_name in db.list_collection_names():
        raise ValueError("-----------------Collection Exists------------------")
    else:
        db.create_collection(collection_name,validator={
            "$jsonSchema":{
                "bsonType": "object",
                "required": ["users_id", "login"],
                "properties": {
                    "users_id": {
                        "bsonType": "int"
                    },
                    "login": {
                        "bsonType": "string"
                    },
                    "gravatar_id": {
                        "bsonType": ["string", "null"]
                    },
                    "avatar_url": {
                        "bsonType" : ["string", "null"]
                    },
                    "url": {
                        "bsonType": ["string", "null"]
                    }
                }
            }
        })

def validate_mongo_schema(collection_name:str, db: Database):
    collections = db.list_collection_names()
    # print (collections)
    if collection_name not in collections:
        raise ValueError("---------------------Missing Collection in DB-----------------------")
    user = db["Users"].find_one({"users_id": 1})
    if not user:
        raise ValueError("---------------------Missing Value in DB-----------------------")
    print("--------------------------Mongo DB Schema Validated----------------------")
    result = db['Users'].delete_many({"users_id": 1})
    print(f"-----------------------Deleted Test Record: {result.deleted_count} Record(s) Deleted-----------------------")
