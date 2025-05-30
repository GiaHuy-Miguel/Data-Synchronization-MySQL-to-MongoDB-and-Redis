import os
from dataclasses import dataclass
from dotenv import load_dotenv


class DatabaseConfig:
    def validate(self) -> None:
        for k,v in self.__dict__.items():
            if v is None:
                raise ValueError(f"----------------Missing Config for {k}--------------------")\

"""
    Dataclass =>> một class chỉ chứa data thôi"
    Ví dụ:  
        `````
         @dataclass
         class MongoDBConfig(DatabaseConfig):
            uri: str
            db_name: str 
        `````
    Sẽ tương tự với:
        `````
        class MongoDBConfig(DatabaseConfig): 
            def __init__ (self, uri, db_name): 
                self.uri = uri 
                self.db_name = db_name
        `````
 """

@dataclass
class MongoDBConfig(DatabaseConfig):
    uri: str
    db_name: str


@dataclass
class MySQLConfig(DatabaseConfig):
    host: str
    url: str
    port: str
    password: str
    driver: str
    database: str
    user: str

@dataclass
class RedisConfig(DatabaseConfig):
    host: str
    port: str
    user: str
    password: str
    database: str

def get_dbconfig() -> dict[str,DatabaseConfig]:
    load_dotenv()
    # print(load_dotenv())

    config = {
        "mongo": MongoDBConfig(
            uri=os.getenv("MONGO_URI"),
            db_name=os.getenv("MONGO_DB_NAME"))
        ,
        "mysql": MySQLConfig(
            host= os.getenv("MYSQL_HOST"),
            url=os.getenv("MYSQL_URL"),
            port= os.getenv("MYSQL_PORT"),
            password= os.getenv("MYSQL_PASSWORD"),
            driver= os.getenv("MYSQL_DRIVER"),
            database= os.getenv("MYSQL_DATABASE"),
            user= os.getenv("MYSQL_USER"))
        ,
        "redis": RedisConfig(
            host= os.getenv("REDIS_HOST"),
            port= os.getenv("REDIS_PORT"),
            user=os.getenv("REDIS_USER") ,
            password= os.getenv("REDIS_PASSWORD"),
            database= os.getenv("REDIS_DATABASE"))
    }

    for db, setting in config.items():
        setting.validate()
    return config

# test_config = get_dbconfig()
# print(test_config)