from pyspark.sql import SparkSession
import os


def custom_filter(row):
    """过滤掉price大于100的行,过滤掉category是Electronics的行
    Args:
        row (_type_): 一行数据

    Returns:
        _type_: 返回True则保留该行,False则为去除
    """
    price = row["price"]
    if price is None:  # 跳过 None 值
        return False
    try:
        if float(price) > 100:
            return False
        elif row["category"] == "Electronics" or row["category"] == "Clothing":
            return False
        else:
            # 必须指明何时返回True，返回True则保留该行
            return True
    except ValueError:
        return False


# 设置环境变量，确保 Python 版本一致
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.11"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3.11"

# 初始化 SparkSession
spark = SparkSession.builder.appName("custom filter").master("local[*]").getOrCreate()

txt_file_path = "file:///root/projects/spark/pyspark-example-project/data/products.csv" 

df = spark.read.csv(txt_file_path, header=True, inferSchema=True)
print(df.show(10))
rdd = df.rdd
filtered_rdd = rdd.filter(custom_filter)
filtered_df = spark.createDataFrame(filtered_rdd, schema=df.schema)
filtered_df.show(10)

spark.stop()
