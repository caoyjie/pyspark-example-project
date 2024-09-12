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
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("udf_test") \
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "true") \
        .getOrCreate()

    # log that main ETL job is starting
    #log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(spark,data)
    hdfs_sink="/sink"
    load_data(data_transformed,hdfs_sink)

    # log the success and terminate Spark application
    #log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = spark.read.csv('/data/sales_data.csv', header=True, inferSchema=True)
    return df


def transform_data(spark,df):
    """
    Apply transformations to each row of the DataFrame:
    - Add a unique ID
    - Execute a command and get its output
    - Add the current timestamp
    """
    from jobs.udf import square_number
    
    # 注册自定义函数
    spark.udf.register("square_number",square_number)

    # 从df创建临时表,注意spark 3.3.2版本这里会报错，需要使用spark3.5.2版本
    df.createOrReplaceTempView("customers")

    square_number_sql="""
        select 
        sale_id,product_name,square_number(price) as square_price,quantity,sale_date,customer_name,customer_email,customer_country
        from customers
        """
    # 必须使用spark3.5.2版本，使用3.3.2版本这里会报错
    result = spark.sql(square_number_sql)
    print(result.show(10))

    return result


def load_data(df, hdfs_path):
    """
    将 DataFrame 保存为 Parquet 格式到 HDFS
    :param df: 要保存的 DataFrame
    :param hdfs_path: HDFS 中的目标路径
    """
    try:
        # 保存 DataFrame 到 HDFS 中的 Parquet 文件
        df.write \
          .mode('overwrite') \
          .parquet(hdfs_path)
        print(f"Data successfully saved as Parquet to {hdfs_path}")
    except Exception as e:
        print(f"An error occurred while saving data to HDFS: {e}")


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
