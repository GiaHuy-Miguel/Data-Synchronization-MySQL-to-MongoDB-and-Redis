from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Connect to Mongo
class MongoConnect:
    def __init__(self, mongo_uri, mongo_dbname):
        self.uri = mongo_uri
        self.dbname = mongo_dbname
        self.client = None
        self.db = None

    # connect method
    def connect(self):
        try:
            self.client = MongoClient(self.uri)
            self.client.server_info()
            self.db = self.client[self.dbname]
            print(self.client.server_info())
            print(f"-----------------Connected to MongoDB: {self.dbname}----------------------------")
            return self.db
        except ConnectionFailure as e:
            raise Exception(f"-------------------------Failed to Connect to MongoDB: {e}-----------------")

    def close(self):
        if self.client:
            self.client.close()
            print("----------------------------------MongoDB Client Closed-------------------------------")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return self

