from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import explode, col
import os

# 设置环境变量，确保 Python 版本一致
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.10'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.10'

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("ReadTXTAndSaveAsParquet") \
    .master("local[*]") \
    .getOrCreate()

# 定义 TXT 文件所在的路径，假设每个文件夹都在 /data/ 下
txt_file_path = "file:///root/projects/spark/pyspark-example-project/data/json_cart_data_1726142841.704159.txt"  # 使用通配符匹配所有文件夹和txt文件

# 读取所有 TXT 文件
rdd = spark.sparkContext.textFile(txt_file_path)

# 每一行是一个 JSON 字符串，将其解析为 DataFrame
json_df = spark.read.json(rdd)

# 有1列叫cart_items，是一个数组，元素是json，解析每一个json，最终达到一行变多行（数组被展开了）
exploded_df = json_df.withColumn("cart_item", explode(col("cart_items")))

#print(exploded_df.show(10))

# 解析cart_item列的每一个json值，生成新的列
final_df = exploded_df.select(
    col("user_id"),    
    col("user_name"),  
    col("email"),               
    col("cart_item.product_name").alias("product_name"), 
    col("cart_item.price").alias("price"),  
    col("cart_item.quantity").alias("quantity"),
    col("cart_item.added_to_cart_at").alias("added_to_cart_at")
)

print(final_df.show(50))

# 显示 DataFrame 的 Schema
final_df.printSchema()

# 将 DataFrame 保存为 Parquet 格式
output_path = "/sink/cart/data_parquet"
final_df.write.parquet(output_path, mode="overwrite")

# 停止 SparkSession
spark.stop()
