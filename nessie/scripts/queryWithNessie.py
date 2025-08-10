from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder \
    .appName("NessieIcebergExample") \
    .config("spark.jars", "/root/nessie-spark-extensions-3.3_2.12-0.103.6.jar,/root/iceberg-spark-runtime-3.3_2.12-1.8.1.jar,/root/iceberg-aws-bundle-1.9.2.jar") \
    .config("spark.sql.extensions",
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.nessie.uri", "http://127.0.0.1:19120/iceberg/main/") \
    .config("spark.sql.catalog.nessie.type", "rest") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.warehouse", "warehouse") \
    .getOrCreate()

spark.sql("SELECT * FROM nessie.default_db3.test_table.history;").show(10)