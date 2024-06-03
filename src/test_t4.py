from datetime import datetime
from Task_1_StreamSimulator import StreamingSimulator
from Task_1_StreamSimulator import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import argparse

class TrendingArrivals(StreamingSimulator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.previous_goldman_batch = None
        self.previous_citigroup_batch = None

    def foreach_batch_function(self, df: DataFrame, epoch_id: int):
        print("====================================Received batch:", epoch_id, "================================")

        # Collect all timestamps in the batch
        time_intervals = df.select("time").distinct().collect()
        time_intervals = sorted([row["time"] for row in time_intervals])

        # Separate DataFrame into two based on 'headquarter' column values
        goldman_df = df.filter(col("headquarter") == "Goldman Sachs").withColumnRenamed("total_count", "goldman_count")
        citigroup_df = df.filter(col("headquarter") == "Citigroup").withColumnRenamed("total_count", "citigroup_count")

        # goldman_df.show(truncate=False)
        # citigroup_df.show(truncate=False)

        # Add a column to mark trend status
        goldman_df = goldman_df.withColumn("is_trend", lit(False))
        citigroup_df = citigroup_df.withColumn("is_trend", lit(False))

        def process_data(df, previous_batch, count_col, previous_batch_attr, city_name):

            if not df.isEmpty():
                # Union the current batch with the previous batch
                df = df.withColumn("time_diff", lit(None).cast("long"))
                df = df.withColumn("count_ratio", lit(None).cast("double"))

                if previous_batch is None:
                    previous_batch = self.spark.createDataFrame([], df.schema)

                # Add columns 'time_diff' and 'count_ratio' to the union
                df = previous_batch.union(df)

                window_spec = Window.partitionBy("headquarter").orderBy("time")
                df = df.withColumn("time_diff", col("time") - lag("time", 1).over(window_spec))
                df = df.withColumn("count_ratio", col(count_col) / lag(count_col, 1).over(window_spec))

                # Identify rows that meet trend criteria and have not been marked as trends yet
                new_trends = df.filter(
                    (col("time_diff") == 600) &
                    (col(count_col) > 10) &
                    (col("count_ratio") >= 2) &
                    (col("is_trend") == False)
                )

                # # Print new trends
                # if new_trends.count() >= 1:
                #     new_trends.show(truncate=False)
                # Print new trends
                if new_trends.count() >= 1:
                    for row in new_trends.collect():
                        initial_count = row[count_col] / row["count_ratio"]
                        new_count = row[count_col]
                        timestamp =  row["time"]  # or the appropriate column name for the timestamp
                        print(f"The number of arrivals to Goldman Sachs has doubled from {int(initial_count)} to {int(new_count)} at {timestamp}!")


                # Mark these rows as trends
                df = df.withColumn("is_trend", when(
                    (col("time_diff") == 600) &
                    (col(count_col) > 10) &
                    (col("count_ratio") >= 2),
                    True
                ).otherwise(col("is_trend")))

                # Update the previous batch attribute with the current batch
                if previous_batch_attr is not None:
                    batch_attr = getattr(self, previous_batch_attr)
                    if batch_attr is not None:
                        batch_attr.unpersist()
                        setattr(self, previous_batch_attr, None)

            # for time in time_intervals:
            #     timestamp = datetime.fromtimestamp(time)
            #     filename = output_path + "part-" + str(timestamp)
            #     current_count = df.filter(col("time") == time).select(F.sum(col(count_col))).collect()[0][0]
            #     previous_count = df.filter(col("time") == time - 600).select(F.sum(col(count_col))).collect()[0][0] if time - 600 in time_intervals else 0
            #     with open(filename, "a") as file:
            #         if previous_count is None:
            #             previous_count = 0
            #         if current_count is None:
            #             current_count = 0
            #         file.write(f"({city_name}, ({current_count}, {timestamp}, {previous_count}))\n")

        
        # Process data for Goldman Sachs and Citigroup
        process_data(goldman_df, self.previous_goldman_batch, "goldman_count", "previous_goldman_batch", "goldman")
        if goldman_df is not None:
            goldman_df.unpersist()
        process_data(citigroup_df, self.previous_citigroup_batch, "citigroup_count", "previous_citigroup_batch", "citigroup")
        if citigroup_df is not None:
            citigroup_df.unpersist()
        del time_intervals

    def query(self, streamingInputDF):
        goldman_bounds = [[-74.0141012, 40.7152191], [-74.013777, 40.7152275], [-74.0141027, 40.7138745], [-74.0144185, 40.7140753]]
        citigroup_bounds = [[-74.011869, 40.7217236], [-74.009867, 40.721493], [-74.010140, 40.720053], [-74.012083, 40.720267]]

        def is_within_bounds(longitude, latitude, bounds):
            try:
                longitude = float(longitude)
                latitude = float(latitude)
            except ValueError:
                return False

            return bounds[0][0] <= longitude <= bounds[1][0] and bounds[2][1] <= latitude <= bounds[0][1]

        def determine_headquarter(longitude, latitude):
            if is_within_bounds(longitude, latitude, goldman_bounds):
                return "Goldman Sachs"
            elif is_within_bounds(longitude, latitude, citigroup_bounds):
                return "Citigroup"
            else:
                return "Others"

        # Register user-defined function
        determine_headquarter_udf = udf(determine_headquarter, StringType())

        # Add 'headquarter' column to the original DataFrame
        streamingInputDF = streamingInputDF.select(
            col("col_1").alias("type"),
            col("col_4").cast("timestamp").alias("dropoff_datetime"),
            when(col("col_1") == 'green', col("col_9"))
                .when(col("col_1") == 'yellow', col("col_11"))
                .alias("dropoff_longitude"),
            when(col("col_1") == 'green', col("col_10"))
                .when(col("col_1") == 'yellow', col("col_12"))
                .alias("dropoff_latitude")
        )        
            
        # Add 'headquarter' column to the original DataFrame
        streamingInputDF = streamingInputDF.withColumn("headquarter", determine_headquarter_udf(col("dropoff_longitude"), col("dropoff_latitude")))

        # Filter out rows where headquarter is null
        streamingInputDF = streamingInputDF.filter(col("headquarter").isNotNull())

        # Group by 'headquarter' and window of 10 minutes
        processed_counts = (
            streamingInputDF
            .withWatermark("dropoff_datetime", "30 minutes")
            .groupBy(
                window(col("dropoff_datetime"), "10 minutes"),
                col("headquarter")
            )
            .count() 
            .withColumnRenamed("count", "total_count")
            .withColumn("time", col("window.start").cast("long"))  # Add 'time' column
        )

        # Start the streaming query
        query = (
            processed_counts
            .writeStream
            .outputMode("append")
            .foreachBatch(self.foreach_batch_function)
            .start()
        )

        return query


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trending Arrivals Spark Job")
    parser.add_argument('--input', required=True, help='Path to input data')
    parser.add_argument('--checkpoint', required=True, help='Path to checkpoint directory')
    parser.add_argument('--output', required=True, help='Path to output directory')
    
    args = parser.parse_args()

    
    input_path = args.input
    checkpoint_path = args.checkpoint
    output_path = args.output

    # input_path = "/home/s21120580/BigData_Lab03/taxi-data"
    # output_path = "/home/s21120580/BigData_Lab03/src/task_4_output/"

    trending = TrendingArrivals(
        java_home="/usr/lib/jvm/java-11-openjdk-amd64",
        spark_home="/home/s21120580/spark-3.5.1-bin-hadoop3",
        hadoop_home="/home/s21120580/hadoop-3.3.6",
        app_name="Trending Arrivals",
        config_option="some-value",
        shuffle_partitions="2",
        input_folder=input_path,
        max_files_per_trigger=60,
    )
    trending.setup_environment()
    trending.initialize_spark()
    trending.define_schema()
    trending.start_streaming()
