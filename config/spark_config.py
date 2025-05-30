import os.path
from typing import Optional
from pyspark.sql import SparkSession

class SparkConnect:
    def __init__(self,
                 app_name: str,
                 master_url: str = "local[*]",
                 # thực tế là đi làm ko được chạy trên local =>> phải link vào con master
                 executor_memory: Optional[str] = '4g',
                 executor_core: int | None = 2,
                 driver_memory: str | None = '2g',
                 numb_executors: int | None = 3,
                 jars: list[str] | None = None,
                 spark_conf: list[dict[str, str]] | None = None,
                 log_level: str = "INFO"
                 ):
        self.app_name = app_name
        self.spark_ = self.create_spark_session(master_url, executor_memory, executor_core, driver_memory, numb_executors, jars, spark_conf, log_level)

    def create_spark_session(self,
            # app_name: str,
            master_url: str = "local[*]",
            # thực tế là đi làm ko được chạy trên local =>> phải link vào con master
            executor_memory: Optional[str] = '4g',
            executor_core: int | None = 2,
            driver_memory: str| None = '2g',
            numb_executors: int | None = 3,
            jars: list[str] | None = None,
            spark_conf: list[dict[str, str]]| None = None,
            log_level: str = "INFO") -> SparkSession: # Vào parameter -> ra SparkSession

            builder = SparkSession.builder \
                .appName(self.app_name) \
                .master(master_url)

            if executor_memory:
                builder.config("spark.executor.memory", executor_memory)
            if executor_core:
                builder.config("spark.executor.cores", executor_core)
            if driver_memory:
                builder.config("spark.driver.memory", driver_memory)
            if numb_executors:
                builder.config("spark.executor.instances", numb_executors)
            if jars:
                jars_path = ",".join([os.path.abspath(jar) for jar in jars ])

            # ['/home/miguel/mysql-connect-j-9.2.0/mysql-connect-j-9.2.0.jar',
            #  '/home/miguel/mysql-connect-j-9.2.0/redis-connect-j-9.2.0.jar',
            #  '/home/miguel/mysql-connect-j-9.2.0/mongodb-connect-j-9.2.0.jar']

            # =>>>>> ['/home/miguel/mysql-connect-j-9.2.0/mysql-connect-j-9.2.0.jar,
            #         /home/miguel/mysql-connect-j-9.2.0/redis-connect-j-9.2.0.jar,
            #         /home/miguel/mysql-connect-j-9.2.0/mongodb-connect-j-9.2.0.jar']
                builder.config("spark.jars", jars_path)
            if spark_conf:
                for conf in spark_conf:
                    for k,v in conf.items():
                        builder.config(k,v)

            spark_session = builder.getOrCreate() # phải start spark trước mới sinh ra log để ghi

            spark_session.sparkContext.setLogLevel(logLevel=log_level)

            return spark_session

    def end_spark(self):
        self.spark_.stop()

    def __enter__(self):
        self.create_spark_session()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_spark()