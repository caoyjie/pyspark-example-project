spark-sql \
  --jars /root/nessie-spark-extensions-3.3_2.12-0.103.6.jar,/root/iceberg-spark-runtime-3.3_2.12-1.8.1.jar,/root/iceberg-aws-bundle-1.9.2.jar \
  --conf spark.sql.extensions=org.projectnessie.spark.extensions.NessieSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.nessie.uri=http://127.0.0.1:19120/iceberg/main/ \
  --conf spark.sql.catalog.nessie.type=rest \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.warehouse=warehouse
