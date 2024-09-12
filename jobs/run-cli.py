"""
    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

    $SPARK_HOME/bin/spark-submit \
    --master local \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

    $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

    $SPARK_HOME/bin/spark-submit \
    --master local \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/run-cli.py
"""

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
from dependencies.spark import start_spark
from pyspark.sql.functions import udf
from dependencies.utils import run_command
from pyspark.sql.types import StringType
import time
from pyspark.sql import Row
from uuid import uuid4

def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark,logger,conf = start_spark(
        app_name='run_commands')

    # log that main ETL job is starting
    #log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    sc = spark.sparkContext
    data = extract_data(spark,sc)
    data_transformed = transform_data(data)
    hdfs_sink="/sink"
    load_data(data_transformed,hdfs_sink)

    # log the success and terminate Spark application
    #log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark,sc):
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    rdd = sc.textFile('/data/commands/commands1.txt')
    
    # Convert each line of the RDD to a Row with column name 'command'
    rdd_rows = rdd.map(lambda line: Row(command=line))
    
    # Convert the RDD to a DataFrame
    df = spark.createDataFrame(rdd_rows)
    
    return df


def transform_data(df):
    """
    Apply transformations to each row of the DataFrame:
    - Add a unique ID
    - Execute a command and get its output
    - Add the current timestamp
    """
    
    # Define a UDF to execute commands and return output
    def execute_command(command):
        return run_command(command)

    # Register UDF with Spark
    execute_command_udf = udf(execute_command, StringType())

    # Define a UDF to generate a unique ID
    def generate_unique_id():
        return str(uuid4())

    # Register UDF with Spark
    generate_unique_id_udf = udf(generate_unique_id, StringType())

    # Add unique ID and command output columns, and set execution time
    transformed_df = df.withColumn('unique_id', generate_unique_id_udf()) \
                       .withColumn('command_output', execute_command_udf(df['command'])) \
                       .withColumn('execution_time', lit(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    return transformed_df


def load_data(df, hdfs_path):
    """
    将 DataFrame 保存为 CSV 格式到 HDFS
    :param df: 要保存的 DataFrame
    :param hdfs_path: HDFS 中的目标路径
    """
    try:
        # 保存 DataFrame 到 HDFS 中的 CSV 文件
        df.write \
          .mode('overwrite') \
          .csv(hdfs_path, header=True)
        print(f"Data successfully saved to {hdfs_path}")
    except Exception as e:
        print(f"An error occurred while saving data to HDFS: {e}")

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
