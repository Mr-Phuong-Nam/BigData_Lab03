import os
import findspark
from Task_1_StreamSimulator import StreamingSimulator
from Task_1_StreamSimulator import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class TrendingArrivals(StreamingSimulator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.previous_goldman_batch = None
        self.previous_citigroup_batch = None

    def foreach_batch_function(self, df: DataFrame, epoch_id: int):
        print("====================================Received batch:", epoch_id, "================================")
        
        # Separate DataFrame into two based on 'headquarter' column values
        goldman_df = df.filter(col("headquarter") == "Goldman Sachs").withColumnRenamed("total_count", "goldman_count")
        citigroup_df = df.filter(col("headquarter") == "Citigroup").withColumnRenamed("total_count", "citigroup_count")

        goldman_df.show(truncate=False)
        citigroup_df.show(truncate=False)
        
        # # Add a column to mark trend status
        # goldman_df = goldman_df.withColumn("is_trend", lit(False))
        # citigroup_df = citigroup_df.withColumn("is_trend", lit(False))

        # def process_data(df, previous_batch, count_col, previous_batch_attr):
        #     # Initialize previous_batch as an empty DataFrame if not set

        #     if not df.isEmpty():
        #         # Union the current batch with the previous batch
        #         df = df.withColumn("time_diff", lit(None).cast("long"))
        #         df = df.withColumn("count_ratio", lit(None).cast("double"))
            
        #         if previous_batch is None:
        #             previous_batch = self.spark.createDataFrame([], df.schema)

        #         # df.show()
        #         #add coulumn time_diff and is_trend=false to union
        #         df = previous_batch.union(df)

        #         window_spec = Window.partitionBy("headquarter").orderBy("time")
        #         df = df.withColumn("time_diff", col("time") - lag("time", 1).over(window_spec))
        #         df = df.withColumn("count_ratio", col(count_col) / lag(count_col, 1).over(window_spec))

        #         # df.show(truncate = False)

        #         # Identify rows that meet trend criteria and have not been marked as trends yet
        #         new_trends = df.filter(
        #             (col("time_diff") == 600) & 
        #             (col(count_col) > 10) & 
        #             (col("count_ratio") >= 2) & 
        #             (col("is_trend") == False)
        #         )
                
        #         # Print new trends
        #         if new_trends.count() >= 1:
        #             new_trends.show(truncate=False)

        #         # Mark these rows as trends
        #         df = df.withColumn("is_trend", when(
        #             (col("time_diff") == 600) & 
        #             (col(count_col) > 10) & 
        #             (col("count_ratio") >= 2), 
        #             True
        #         ).otherwise(col("is_trend")))
                
        #         # Update the previous batch attribute with the current batch
        #         # Giả sử self.previous_batch_attr là tên của thuộc tính DataFrame bạn muốn giải phóng
        #         if previous_batch_attr is not None:
        #             batch_attr = getattr(self, previous_batch_attr)
        #             if batch_attr is not None:  # Kiểm tra xem DataFrame đã được tạo trước đó hay chưa
        #                 batch_attr.unpersist()  # Giải phóng bộ nhớ
        #                 setattr(self, previous_batch_attr, None)  # Gán giá trị None cho thuộc tính để không giữ bất kỳ tham chiếu nào

        
        # # Process data for Goldman Sachs and Citigroup
        # process_data(goldman_df, self.previous_goldman_batch, "goldman_count", "previous_goldman_batch")
        # process_data(citigroup_df, self.previous_citigroup_batch, "citigroup_count", "previous_citigroup_batch")


    # def query(self, streamingInputDF):
    #     goldman_bounds = [[-74.0141012, 40.7152191], [-74.013777, 40.7152275], [-74.0141027, 40.7138745], [-74.0144185, 40.7140753]]
    #     citigroup_bounds = [[-74.011869, 40.7217236], [-74.009867, 40.721493], [-74.010140, 40.720053], [-74.012083, 40.720267]]

    #     def is_within_bounds(longitude, latitude, bounds):
    #         try:
    #             longitude = float(longitude)
    #             latitude = float(latitude)
    #         except ValueError:
    #             return False

    #         return bounds[0][0] <= longitude <= bounds[1][0] and bounds[2][1] <= latitude <= bounds[0][1]

    #     def determine_headquarter(longitude, latitude):
    #         if is_within_bounds(longitude, latitude, goldman_bounds):
    #             return "Goldman Sachs"
    #         elif is_within_bounds(longitude, latitude, citigroup_bounds):
    #             return "Citigroup"
    #         else:
    #             return None

    #     # Register user-defined function
    #     determine_headquarter_udf = udf(determine_headquarter, StringType())

    #     # Add 'headquarter' column to the original DataFrame
    #     streamingInputDF = streamingInputDF.select(
    #         col("col_1").alias("type"),
    #         col("col_4").cast("timestamp").alias("dropoff_datetime"),
    #         when(col("col_1") == 'green', col("col_9"))
    #             .when(col("col_1") == 'yellow', col("col_11"))
    #             .alias("dropoff_longitude"),
    #         when(col("col_1") == 'green', col("col_10"))
    #             .when(col("col_1") == 'yellow', col("col_12"))
    #             .alias("dropoff_latitude")
    #     )        
            
    #     # Add 'headquarter' column to the original DataFrame
    #     streamingInputDF = streamingInputDF.withColumn("headquarter", determine_headquarter_udf(col("dropoff_longitude"), col("dropoff_latitude")))

    #     # Filter out rows where headquarter is null
    #     streamingInputDF = streamingInputDF.filter(col("headquarter").isNotNull())

    #     # Group by 'headquarter' and window of 10 minutes
    #     processed_counts = (
    #         streamingInputDF
    #         .withWatermark("dropoff_datetime", "30 minutes")
    #         .groupBy(
    #             window(col("dropoff_datetime"), "10 minutes"),
    #             col("headquarter")
    #         )
    #         .count() 
    #         .withColumnRenamed("count", "total_count")
    #         .withColumn("time", col("window.start").cast("long"))  # Add 'time' column
    #     )

        
    #     # Start the streaming query
    #     query = (
    #         processed_counts
    #         .writeStream
    #         .outputMode("append")
    #         .foreachBatch(self.foreach_batch_function)
    #         .start()
    #     )

    #     return query
    def query(self, streamingInputDF):
        # Define headquarters bounds
        goldman_bounds = [[-74.0141012, 40.7152191], [-74.013777, 40.7152275], [-74.0141027, 40.7138745], [-74.0144185, 40.7140753]]
        citigroup_bounds = [[-74.011869, 40.7217236], [-74.009867, 40.721493], [-74.010140, 40.720053], [-74.012083, 40.720267]]

        # Define function to check if coordinates are within bounds
        def is_within_bounds(longitude, latitude, bounds):
            try:
                longitude = float(longitude)
                latitude = float(latitude)
            except ValueError:
                return False

            return bounds[0][0] <= longitude <= bounds[1][0] and bounds[2][1] <= latitude <= bounds[0][1]

        # Define function to determine headquarters
        def determine_headquarter(longitude, latitude):
            if is_within_bounds(longitude, latitude, goldman_bounds):
                return "Goldman Sachs"
            elif is_within_bounds(longitude, latitude, citigroup_bounds):
                return "Citigroup"
            else:
                return None

        # Register user-defined function
        # determine_headquarter_udf = udf(determine_headquarter, StringType())

        # # Select relevant columns and add 'headquarter' column
        # streamingInputDF = streamingInputDF.select(
        #     col("col_1").alias("type"),
        #     col("col_4").cast("timestamp").alias("dropoff_datetime"),
        #     when(col("col_1") == 'green', col("col_9"))
        #         .when(col("col_1") == 'yellow', col("col_11"))
        #         .alias("dropoff_longitude"),
        #     when(col("col_1") == 'green', col("col_10"))
        #         .when(col("col_1") == 'yellow', col("col_12"))
        #         .alias("dropoff_latitude")
        # ).withColumn("headquarter", determine_headquarter_udf(col("dropoff_longitude"), col("dropoff_latitude")))

        # # Filter out rows where headquarter is null
        # streamingInputDF = streamingInputDF.filter(col("headquarter").isNotNull())

        # # Group by 'headquarter' and window of 10 minutes
        # processed_counts = (
        #     streamingInputDF
        #     .withWatermark("dropoff_datetime", "30 minutes")
        #     .groupBy(
        #         window(col("dropoff_datetime"), "10 minutes"),
        #         col("headquarter")
        #     )
        #     .count() 
        #     .withColumnRenamed("count", "total_count")
        #     .withColumn("time", col("window.start").cast("long"))  # Add 'time' column
        # )

        # # Start the streaming query
        # query = (
        #     processed_counts
        #     .writeStream
        #     .outputMode("append")
        #     .foreachBatch(self.foreach_batch_function)
        #     .start()
        # )

        # return query
        # Register UDF
        determine_headquarter_udf = udf(self.determine_headquarter, StringType())

        # Apply UDF and filter out rows where headquarter is null
        streamingInputDF = streamingInputDF.select(
            col("col_1").alias("type"),
            col("col_4").cast("timestamp").alias("dropoff_datetime"),
            when(col("col_1") == 'green', col("col_9"))
                .when(col("col_1") == 'yellow', col("col_11"))
                .alias("dropoff_longitude"),
            when(col("col_1") == 'green', col("col_10"))
                .when(col("col_1") == 'yellow', col("col_12"))
                .alias("dropoff_latitude")
        ).withColumn("headquarter", determine_headquarter_udf(col("dropoff_longitude"), col("dropoff_latitude")))\
        .filter(col("headquarter").isNotNull())

        # Group by 'headquarter' and window of 10 minutes
        processed_counts = (
            streamingInputDF
            .groupBy(
                window(col("dropoff_datetime"), "10 minutes"),
                col("headquarter")
            )
            .count() 
            .withColumnRenamed("count", "total_count")
            .withColumn("time", col("window.start").cast("long"))  # Add 'time' column
            .withWatermark("dropoff_datetime", "30 minutes")
            .groupBy("time", "headquarter")
            .agg(F.sum("total_count").alias("total_count"))
            .orderBy("time", "headquarter")
            .complete(["time", "headquarter"])
            .fillna(0, subset=["total_count"])
        )

        # Print or further process the DataFrame
        processed_counts.show(truncate=False)

        query = (
            processed_counts
            .writeStream
            .outputMode("append")
            .foreachBatch(self.foreach_batch_function)
            .start()
        )

        return query



if __name__ == "__main__":
    input_path = "/home/s21120580/BigData_Lab03/taxi-data"
    output_path = "/home/s21120580/BigData_Lab03/src/task_4_output"

    trending = TrendingArrivals(
        java_home="/usr/lib/jvm/java-11-openjdk-amd64",
        spark_home="/home/s21120580/spark-3.5.1-bin-hadoop3",
        hadoop_home="/home/s21120580/hadoop-3.3.6",
        app_name="Trending Arrivals",
        config_option="some-value",
        shuffle_partitions="1",
        input_folder=input_path,
        max_files_per_trigger=60,
    )
    trending.setup_environment()
    trending.initialize_spark()
    trending.define_schema()
    trending.start_streaming()
