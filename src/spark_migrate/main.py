from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

from config.database_config import get_dbconfig
from config.spark_config import SparkConnect
from data_migration.cdc_mysql import KafkaMySQLConnect
from data_migration.mongo_migration import DataMigration


def main():
    DB_CONFIG = get_dbconfig()

    JARS = ["/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/mysql-connector-j-9.2.0/mysql-connector-j-9.2.0.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/mongo-spark-connector_2.12-10.3.0.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/mongodb-driver-sync-4.11.0.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/bson-4.11.0.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/mongodb-driver-core-4.11.0.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/spark-redis_2.12-3.1.0-jar-with-dependencies.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/spark-sql-kafka-0-10_2.12-3.5.5.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/kafka-clients-3.0.0.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/spark-tags_2.12-3.5.5.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/spark-token-provider-kafka-0-10_2.12-3.5.5.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/commons-pool2-2.11.1.jar",
            "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/jsr305-3.0.0.jar"]

    SCHEMA = StructType([
        StructField("schema", StructType([
            StructField("type", StringType(), True),
            StructField("fields", ArrayType(StructType([
                StructField("type", StringType(), True),
                StructField("fields", ArrayType(StructType([
                    StructField("type", StringType(), True),
                    StructField("optional", BooleanType(), False),
                    StructField("field", StringType(), True),
                ]), True)),
                StructField("name", StringType(), True),
                StructField("field", StringType(), True)
                ]), True), True)
        ]), True),

        StructField("payload", StructType([
            StructField("users_id", IntegerType(), True),
            StructField("before", StructType([
                StructField("users_id", IntegerType(), True),
                StructField("login", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("avatar_url", StringType(), True)
            ]), True),
            StructField("after", StructType([
                StructField("users_id", IntegerType(), True),
                StructField("login", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("avatar_url", StringType(), True)
            ]), True)
        ]), True)
    ])

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

    # kafka = KafkaMySQLConnect(spark,SCHEMA)
    # msg = kafka.get_data_change(SCHEMA)
    # raw = kafka.write_data_change(msg, "console", "append")

    updated_data = DataMigration(spark, SCHEMA, "Users")
    updated_data.start_streaming(updated_data.raw_msg, updated_data.migrate, "append")


if __name__ == "__main__":
    main()
