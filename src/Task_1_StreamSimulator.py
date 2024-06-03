import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

class StreamingSimulator:
    def __init__(self, java_home, spark_home, hadoop_home, app_name, config_option, shuffle_partitions, input_folder, max_files_per_trigger):
        self.java_home = java_home
        self.spark_home = spark_home
        self.hadoop_home = hadoop_home
        self.app_name = app_name
        self.config_option = config_option
        self.shuffle_partitions = shuffle_partitions
        self.input_folder = input_folder
        self.max_files_per_trigger = max_files_per_trigger
        self.spark = None
        self.rawSchema = None

    def setup_environment(self):
        os.environ["JAVA_HOME"] = self.java_home
        os.environ["SPARK_HOME"] = self.spark_home
        os.environ["HADOOP_HOME"] = self.hadoop_home

    def initialize_spark(self):
        findspark.init()
        self.spark = SparkSession.builder.master("local")\
                                  .appName(self.app_name)\
                                  .config("spark.some.config.option", self.config_option)\
                                  .getOrCreate()
        self.spark.sparkContext.setLogLevel("FATAL")
        self.spark.conf.set("spark.sql.shuffle.partitions", self.shuffle_partitions)

    def define_schema(self):
        self.rawSchema = StructType([StructField(f"col_{i}", StringType(), True) for i in range(1, 23)])

    def read_stream(self):
        streamingInputDF = (
            self.spark.readStream
            .option("mode", "PERMISSIVE")
            .schema(self.rawSchema)
            .option("maxFilesPerTrigger", self.max_files_per_trigger)
            .option("header", "false")
            .option("latestFirst", "false")
            .csv(self.input_folder)
        )
        return streamingInputDF

    def query(self, streamingInputDF):
        query = (
            streamingInputDF.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .start()
        )
        return query

    def start_streaming(self):
        streamingInputDF = self.read_stream()
        query = self.query(streamingInputDF)
        query.awaitTermination(60)
        query.stop()

# Usage example
if __name__ == "__main__":
    demo = StreamingSimulator(
        java_home="/usr/lib/jvm/java-11-openjdk-amd64",
        spark_home="/home/phuongnam/Documents/Ki2_nam3/Big_Data/spark-3.5.1-bin-hadoop3",
        hadoop_home="/home/phuongnam/Documents/Ki2_nam3/Big_Data/hadoop-3.3.6",
        app_name="Spark Streaming Demonstration",
        config_option="some-value",
        shuffle_partitions="2",
        input_folder="../taxi-data/",
        max_files_per_trigger=60
    )
    demo.setup_environment()
    demo.initialize_spark()
    demo.define_schema()
    demo.start_streaming()
