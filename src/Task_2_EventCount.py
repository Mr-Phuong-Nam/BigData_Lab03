from Task_1_StreamSimulator import StreamingSimulator
from Task_1_StreamSimulator import *

class EventCountStreaming(StreamingSimulator):
    def foreach_batch_function(self, df, epoch_id):
        if df.count() == 0:
            return
        
        df = df.withColumn("window_start", col("window.start").cast("string"))
        df = df.withColumn("window_end", col("window.end").cast("string"))
        df = df.drop("window")

        start_hour = df.select("window_start").collect()[0][0].split(" ")[1].split(":")[0]
        start_hour = (int(start_hour) + 1) * 360000
        df.persist()
        df.write.mode("overwrite").format("json").save("task_2_output/output-{0}".format(start_hour))
        df.unpersist()
        
    def query(self, streamingInputDF):
        streamingInputDF = streamingInputDF.select(col("col_4").cast("timestamp").alias("dropoff_datetime"))

        windowedCounts = (
            streamingInputDF
            .withWatermark("dropoff_datetime", "30 minutes")
            .groupBy(
                window(col("dropoff_datetime"), "1 hour"))
            .count()
        )

        query = (
            windowedCounts
            .writeStream
            .outputMode("append")
            .foreachBatch(self.foreach_batch_function)
            .start()
        )
        return query


if __name__ == "__main__":
    program = EventCountStreaming(
        java_home="/usr/lib/jvm/java-11-openjdk-amd64",
        spark_home="/home/phuongnam/Documents/Ki2_nam3/Big_Data/spark-3.5.1-bin-hadoop3",
        hadoop_home="/home/phuongnam/Documents/Ki2_nam3/Big_Data/hadoop-3.3.6",
        app_name="Event Count Streaming",
        config_option="some-value",
        shuffle_partitions="2",
        input_folder="../taxi-data/",
        max_files_per_trigger=60
    )
    program.setup_environment()
    program.initialize_spark()
    program.define_schema()
    program.start_streaming()

