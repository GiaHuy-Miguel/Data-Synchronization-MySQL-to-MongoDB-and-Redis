from traceback import print_tb

from bson import Int64
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from config.database_config import get_dbconfig
from data_migration.cdc_mysql import KafkaMySQLConnect
from database_connect.mongo_connect import MongoConnect
from database_connect.redis_connect import RedisConnect


class DataMigration(KafkaMySQLConnect):
    def __init__(self, spark: SparkSession, schema: StructType, collection:str):
        super(DataMigration, self).__init__(spark, schema)
        self.collection = collection

    def migrate(self, raw_msg: DataFrame, epoch_id):
        raw_msg.persist()

        config_mongo = get_dbconfig()

        raw_msg.show()

        # inserts = raw_msg.filter(col("data_change") == "insert") \
        #     .select(col("data_after.users_id").alias("users_id"),
        #             col("data_after.login").alias("login"),
        #             col("data_after.gravatar_id").alias("gravatar_id"),
        #             col("data_after.url").alias("url"),
        #             col("data_after.avatar_url").alias("avatar_url"))

        upserts = raw_msg.filter(col("data_change") != "delete" ) \
            .select(col("data_after.users_id").alias("users_id"),
                    col("data_after.login").alias("login"),
                    col("data_after.gravatar_id").alias("gravatar_id"),
                    col("data_after.url").alias("url"),
                    col("data_after.avatar_url").alias("avatar_url"))

        deletes = raw_msg.filter(col("data_change") == "delete") \
            .select(col("data_before.users_id").alias("users_id"),
                    col("data_before.login").alias("login"),
                    col("data_before.gravatar_id").alias("gravatar_id"),
                    col("data_before.url").alias("url"),
                    col("data_before.avatar_url").alias("avatar_url"))

        # CONNECT TO MONGODB
        mongodb_client = MongoConnect(config_mongo["mongo"].uri, config_mongo["mongo"].db_name)

        #CONNECT TO REDIS
        redis_config = get_dbconfig()["redis"].__dict__
        redis_client = RedisConnect(**redis_config)


        if not deletes.isEmpty():
            ids = [num[0] for num in deletes.select("users_id").collect()]

        # Delete record(s) in Mongo
            delete_count_mongo = mongodb_client.connect()['Users'].delete_one({'users_id': {'$in': ids}})
        # Delete record(s) in Redis
            redis_client.connect().delete(*ids)
            delete_count_redis = redis_client.connect().hgetall(*ids)

            if delete_count_mongo and not delete_count_redis:
                print(f"----------------------------- Deleted Record(s) Successfully------------------------")

        if not upserts.isEmpty():
            upserts.write.format("mongodb") \
                    .option("database", config_mongo["mongo"].db_name) \
                    .option("collection",  self.collection) \
                    .option("mongodb.output.uri",config_mongo["mongo"].uri) \
                    .mode("append") \
                    .save()

            upserts.write.format("org.apache.spark.sql.redis") \
                .option("table", "redis_table") \
                .option("key.column", "users_id") \
                .mode("append") \
                .save()

            print("--------------------- Updated/Inserted Record(s) Successfully ----------------------")

        # if not updates.isEmpty():
        #     updates.write.format("mongodb") \
        #             .option("database", config_mongo["mongo"].db_name) \
        #             .option("collection",  self.collection) \
        #             .option("mongodb.output.uri",config_mongo["mongo"].uri) \
        #             .option("operationType","replace") \
        #             .mode("append") \
        #             .save()
        #     print("------------------------- Updated Successfully ----------------------")

        raw_msg.unpersist()
