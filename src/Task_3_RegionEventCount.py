from dataclasses import dataclass
from os.path import expanduser

from pyspark.sql import  functions as F
from Task_1_StreamSimulator import StreamingSimulator


@dataclass
class Headquarter:
    goldman_bb = [[-74.0141012, 40.7152191], [-74.013777, 40.7152275], [-74.0141027, 40.7138745], [-74.0144185, 40.7140753]] 
    citigroup_bb = [[-74.011869, 40.7217236], [-74.009867, 40.721493], [-74.010140,40.720053], [-74.012083, 40.720267]] 

    # [x_min, x_max, y_min, y_max]
    goldman_region = [min([coord[0] for coord in goldman_bb]), max([coord[0] for coord in goldman_bb]), min([coord[1] for coord in goldman_bb]), max([coord[1] for coord in goldman_bb])]
    citigroup_region = [min([coord[0] for coord in citigroup_bb]), max([coord[0] for coord in citigroup_bb]), min([coord[1] for coord in citigroup_bb]), max([coord[1] for coord in citigroup_bb])]

headquarter = Headquarter()

class RegionEventCount(StreamingSimulator):

    @staticmethod
    def in_region(col_1, col_9, col_10, col_11, col_12):
        if col_1 == 'yellow':
            x, y = float(col_11), float(col_12)
        elif col_1 == 'green':
            x, y = float(col_9), float(col_10)
        else:
            return None
        
        if headquarter.goldman_region[0] <= x <= headquarter.goldman_region[1] and headquarter.goldman_region[2] <= y <= headquarter.goldman_region[3]:
            return 'goldman'
        elif headquarter.citigroup_region[0] <= x <= headquarter.citigroup_region[1] and headquarter.citigroup_region[2] <= y <= headquarter.citigroup_region[3]:
            return 'citigroup'
        
        return None

    def for_each_function(self, df, epoch_id):
        if df.count() == 0:
            return

        df = df.filter(F.col('headquarter').isNotNull())
        # df.show()
        df = df.withColumn('window_start', F.col('window.start').cast('string'))
        df = df.withColumn('window_end', F.col('window.end').cast('string'))
        df = df.drop('window')

        start_hour = df.select('window_start').collect()[0][0].split(' ')[1].split(':')[0]
        start_hour = (int(start_hour) + 1) * 360000
        df.persist()
        df.select('headquarter', 'count').write.mode('overwrite').format('csv').save(f'task_3_output/output-{start_hour}')
        df.unpersist()

    def query(self, streamingInputDF):
        udf_in_region = F.udf(self.in_region)
        streamingInputDF = streamingInputDF.withColumn('dropoff_datetime', F.col('col_4').cast('timestamp'))
        streamingInputDF = streamingInputDF.withColumn('headquarter', udf_in_region('col_1', 'col_9', 'col_10', 'col_11', 'col_12'))
        streamingInputDF = streamingInputDF.select('dropoff_datetime', 'headquarter')

        _windowedCounts = (
            streamingInputDF
            .withWatermark('dropoff_datetime', '30 minutes')
            .groupBy(F.window(F.col('dropoff_datetime'), '1 hour'), F.col('headquarter'))
            .count()
        )

        _query = (
            _windowedCounts
            .writeStream
            .outputMode('append')
            .foreachBatch(self.for_each_function)
            .start()
        )

        return _query


if __name__ == '__main__':
    program = RegionEventCount(
        java_home = '/usr/lib/jvm/java-17-openjdk',
        spark_home = expanduser('~/miniconda3/envs/pyspark/lib/python3.12/site-packages/pyspark'),
        hadoop_home = 'foo',
        app_name = 'RegionEventCount',
        config_option = 'some-value',
        shuffle_partitions = '2',
        input_folder = '../taxi-data/',
        max_files_per_trigger = 60
    )

    program.setup_environment()
    program.initialize_spark()
    program.define_schema()
    program.start_streaming()