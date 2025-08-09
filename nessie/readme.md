# Nessie Catalog

Use nessie as catalog service and s3 as backend

## preqreist

### JDK

```bash
openjdk 17.0.15 2025-04-15
OpenJDK Runtime Environment (build 17.0.15+6-Ubuntu-0ubuntu122.04)
OpenJDK 64-Bit Server VM (build 17.0.15+6-Ubuntu-0ubuntu122.04, mixed mode, sharing)
```

### Scala

```bash
Scala code runner version 2.11.12 -- Copyright 2002-2017, LAMP/EPFL
```

### spark

Need spark-3.3.0-bin-hadoop3.tar

tar to unzip(/opt/spark-local) and add env args below to /etc/profile

```
export SPARK_HOME=/opt/spark-local
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

`source /etc/profile`

### Pyspark

pyspark=3.3.0

### jar

- [nessie-spark-extensions-3.3_2.12](https://mvnrepository.com/artifact/org.projectnessie.nessie-integrations/nessie-spark-extensions-3.3_2.12/0.103.6)

- [iceberg-spark-runtime-3.3_2.12](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.3_2.12/1.8.1)

## docker compose

`docker compose -f /root/project/pyspark-example-project/nessie/docker-compose-nessie-mongoDB.yaml up`

Use this yaml to run docker compose

```yaml
version: '3'
services:
  nessie:
    image: ghcr.io/projectnessie/nessie:0.104.3
    ports:
      # API port
      - "19120:19120"
      # Management port (metrics and health checks)
      - "9000:9000"
    depends_on:
      - mongo
    environment:
      - nessie.version.store.type=MONGODB
      - nessie.catalog.warehouses.warehouse.location=s3://my-datawarehouse
      - nessie.catalog.default-warehouse=warehouse
      - nessie.catalog.service.s3.default-options.access-key=urn:nessie-secret:quarkus:my-secrets.s3-default
      - my-secrets.s3-default.name=<key>
      - my-secrets.s3-default.secret=<secret>
      - my-secrets.s3-default.region=us-east-1
      - nessie.catalog.service.s3.default-options.region=us-east-1
      - quarkus.mongodb.database=nessie
      - quarkus.mongodb.connection-string=mongodb://root:password@mongo:27017
  mongo:
    image: mongo
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: root
      MONGO_INITDB_ROOT_PASSWORD: password
    volumes:
      - mongo_data:/data/db

volumes:
  mongo_data:
```

## spark-sql

```bash
spark-sql \
  --jars /root/nessie-spark-extensions-3.3_2.12-0.103.6.jar,/root/iceberg-spark-runtime-3.3_2.12-1.8.1.jar,/root/iceberg-aws-bundle-1.9.2.jar \
  --conf spark.sql.extensions=org.projectnessie.spark.extensions.NessieSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.nessie.uri=http://127.0.0.1:19120/iceberg/main/ \
  --conf spark.sql.catalog.nessie.type=rest \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.warehouse=warehouse
```

## PyScript Example

```python
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

# # 创建数据库（namespace）
# spark.sql("CREATE DATABASE IF NOT EXISTS nessie.default_db1")

# # 创建表
spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.default_db1.test_table (
    id INT,
    name STRING,
    ts TIMESTAMP
) USING iceberg
""")

# 插入数据
spark.sql("""
INSERT INTO nessie.default_db1.test_table VALUES
    (1, 'Alice', current_timestamp()),
    (2, 'Bob', current_timestamp())
""")

# 查询数据
df = spark.sql("SELECT * FROM nessie.default_db1.test_table")
df.show()

spark.stop()
```