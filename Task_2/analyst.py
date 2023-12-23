import os
import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

def main():
    spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('Streaming Process Files') \
        .config('spark.sql.streaming.forceDeleteTempCheckpointLocation', True) \
        .config('spark.sql.streaming.statefulOperator.checkCorrectness.enabled', False) \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    schema = StructType().add('time', 'string').add('channel_name', 'string').add('text', 'string').add('media', 'boolean')

    df = spark \
        .readStream \
        .option('cleanSource', 'delete') \
        .schema(schema) \
        .parquet('Task_2/data') \
        .withColumn('time_lapse', F.input_file_name())
    
    query = df \
        .groupBy('time_lapse', 'channel_name') \
        .count() \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .option('truncate', 'false') \
        .start()
    
    while True:
        pass

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'C:\Program Files\Java\jdk-20.0.1'
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    main()