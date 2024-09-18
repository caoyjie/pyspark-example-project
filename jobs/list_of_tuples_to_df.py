from pyspark.sql import SparkSession
"""
将一个列表（元素为元组），转换为DataFrame

"""
# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("list_of_tuples_to_df") \
    .master("local[*]") \
    .getOrCreate()

list_tuple=list=[("david",25,"male"),("Nancy",33,"female")]

rdd=spark.sparkContext.parallelize(list_tuple)

print(rdd.take(2))

columns=['name','age','gender']

df=spark.createDataFrame(rdd,columns)

df.show(2)
df.printSchema()

spark.stop()