from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from config.database_config import DatabaseConfig

"""
    THIS FILE COVERS SPARK FUNCTIONS IN RELATING TO WRITE & READ DATA FROM DATABASES
    IT ALSO INCLUDES DATA VALIDATION WHEN IT IS IMPORTED TO THE DATABASE 
"""

class ValidateImport:
    def __init__(self):
        self.check_df = None
    def validate_import(self, data_source: DataFrame):
        # db_data = self.read_mysql()
        check1 = self.check_df.exceptAll(data_source)
        check2 = data_source.exceptAll(self.check_df)
        if not check1.isEmpty():
            raise Exception("------------------------- Import Failure: Have Not Deleted Test Data -----------------")
        if not check2.isEmpty():
            raise Exception("------------------------- Import Failure: Missing Data -----------------")
        print("-------------------- Import Validated ----------------------")


class SparkConnectMySQL(ValidateImport):
    def __init__(self, spark: SparkSession, table_name: str, db_config: dict[str,DatabaseConfig]):
        super().__init__()
        self.db_config = db_config
        self.spark_ = spark
        self.table_name = table_name
        self.check_df = self.read_mysql()

    def read_mysql(self):
        df = self.spark_.read \
            .format("jdbc") \
            .option("url", self.db_config["mysql"].url) \
            .option("dbtable", self.table_name) \
            .option("user", self.db_config["mysql"].user) \
            .option("password", self.db_config["mysql"].password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        return df

    def write_mysql(self, df: DataFrame, properties):
        check = df.subtract(self.check_df)
        # check.show()
        if check.isEmpty():
            raise KeyError("--------------- KEY EXISTED EHEHEHE -------------------")

        properties.update({"user": self.db_config["mysql"].user,
                           "password": self.db_config["mysql"].password,
                           "driver": "com.mysql.cj.jdbc.Driver"})
        # print(properties)
        df.write.jdbc(
            url= self.db_config["mysql"].url,
            table= self.table_name,
            mode="append",
            properties=properties
        )
        print("---------------- MySQL Written with Spark -----------------")
        self.validate_import(df)


class SparkConnectMongoDB(ValidateImport):
    def __init__(self, spark: SparkSession, collection: str, db_config: dict[str, DatabaseConfig]):
        super().__init__()
        self.db_config = db_config
        self.spark_ = spark
        self.db_name = db_config["mongo"].db_name
        self.collection = collection
        self.check_df = self.read_mongodb()

    def read_mongodb(self):
        df = self.spark_.read.format("mongodb") \
            .option("database",  self.db_name) \
            .option("collection",  self.collection) \
            .load()
        result = df.select(col('avatar_url'), col('gravatar_id'), col('login'), col('url'), col('users_id'))
        return result
    def write_mongodb(self, df: DataFrame, mode: str):
        try:
            if self.collection == "Users":
                df.write.format("mongodb") \
                    .option("database", self.db_name) \
                    .option("collection",  self.collection) \
                    .mode(mode) \
                    .save()
                print("---------------- MongoDB Written with Spark -----------------")
                self.validate_import(df)
        except ConnectionError as e:
            print(f"--------------------- Connection failed: {e} ---------------------------")


class SparkConnectRedis(ValidateImport):
    def __init__(self, spark: SparkSession, db_config: dict[str, DatabaseConfig] ,table: str, key_column: str, schema: StructType|None = None):
        super().__init__()
        self.table = table
        self.key_column = key_column
        self.spark = spark
        self.schema = schema
        self.database = db_config["redis"].database
        self.check_df = self.read_redis()

    def write_redis(self, df: DataFrame):
        check = df.subtract(self.check_df)
        if check.isEmpty():
            raise KeyError("--------------- KEY EXISTED EHEHEHE -------------------")
        df.write.format("org.apache.spark.sql.redis") \
            .option("table", self.table) \
            .option("key.column", self.key_column) \
            .save()
        print(f"------------------------ Redis Written with Spark --------------------------------")
        self.validate_import(df)

    def read_redis(self):
        if not self.schema:
            df = self.spark.read.format("org.apache.spark.sql.redis") \
                .option("inferSchema", True) \
                .option("database",self.database) \
                .option("table", self.table) \
                .option("key.column", self.key_column) \
                .load()
        else:
            df = self.spark.read.format("org.apache.spark.sql.redis") \
                .schema(self.schema) \
                .option("database", self.database) \
                .option("table", self.table) \
                .option("key.column", self.key_column) \
                .load()
        return df