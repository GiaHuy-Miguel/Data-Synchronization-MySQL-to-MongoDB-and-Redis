from config.database_config import get_dbconfig
from config.spark_config import SparkConnect
from database_connect.spark_db_connect import SparkConnectMySQL, SparkConnectMongoDB, SparkConnectRedis

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

"""
    THIS FILE IS TO UPDATE GIVEN DATA TO ALL DATABASES: MYSQL, MONGODB, REDIS
"""

def main():
    DB_CONFIG = get_dbconfig()

    JARS = ["/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/mysql-connector-j-9.2.0/mysql-connector-j-9.2.0.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/mongo-spark-connector_2.12-10.3.0.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/mongodb-driver-sync-4.11.0.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/bson-4.11.0.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/mongodb-driver-core-4.11.0.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/spark-redis_2.12-3.1.0-jar-with-dependencies.jar"]

    PATH = "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/data/2015-03-01-17.json"

    SCHEMA = StructType([
        StructField("actor", StructType([
            StructField("id", IntegerType(), True),
            StructField("login", StringType(), True),
            StructField("gravatar_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("avatar_url", StringType(), True)
        ]), True)
    ])

    PROPERTIES = {
        # Cache prepared statements for reuse & batch processing
        "cachePrepStmts": "true",  # Cache prepared statements
        "prepStmtCacheSize": "250",  # Size of prepared statement cache
        "prepStmtCacheSqlLimit": "2048",  # Max SQL length for caching
        "useServerPrepStmts": "true",  # Use server-side prepared statements
        "rewriteBatchedStatements": "true",  # Optimize batch inserts
        "connectTimeout": "5000",  # Timeout for establishing connection
        "socketTimeout": "30000"  # Timeout for socket operations
    }

# CONNECT TO SPARK
    spark = SparkConnect(
        app_name="ballerina capuccina mimimi",
        master_url="local[*]", # thực tế là đi làm ko được chạy trên local =>> phải link vào con master
        executor_memory='4g',
        executor_core=2,
        driver_memory='2g',
        numb_executors=3,
        jars=JARS,
        spark_conf= [{"spark.mongodb.output.uri": DB_CONFIG["mongo"].uri},
                     {"spark.redis.host": DB_CONFIG["redis"].host},
                     {"spark.redis.auth": DB_CONFIG["redis"].password},
                     {"spark.redis.port": DB_CONFIG["redis"].port}],
        log_level= "WARN").spark_

# READ JSON FILE
    user_df = spark.read. \
        schema(SCHEMA). \
        json(PATH)
    user_df = user_df.select(col("actor.id").alias("users_id"),
                             col("actor.login"),
                             col("actor.gravatar_id"),
                             col("actor.url"),
                             col("actor.avatar_url")).distinct()
    user_df.show()


# UPLOAD TO DB
# ----------------------------- MYSQL -------------------------------------------
    spark_mysql = SparkConnectMySQL(spark,"GIT.Users" ,DB_CONFIG)
    spark_mysql.write_mysql(user_df, PROPERTIES)
    # spark_mysql.read_mysql()

# ----------------------------- MONGODB ------------------------------------------
    spark_mongo = SparkConnectMongoDB(spark, "Users", DB_CONFIG)
    spark_mongo.write_mongodb(user_df, "overwrite")
    # spark_mongo.read_mongodb().show()

# ----------------------------- REDIS --------------------------------------------
    spark_redis = SparkConnectRedis(spark, DB_CONFIG,"redis_table" ,"users_id")
    spark_redis.write_redis(user_df)
    # spark_redis.read_redis()

if __name__ == "__main__":
    main()