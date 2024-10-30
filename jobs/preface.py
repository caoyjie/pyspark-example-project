from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import explode, col,udf
import os


def preface_splited(str):
    """上下文切分
    示例输入：技术,技术总监,技术总监下面,技术总监下面有,技术总监下面有各个,技术总监下面有各个项目组
    返回：技术#总监#下面#有#各个#项目组

    Args:
        str (_type_): _description_

    Returns:
        _type_: _description_
    """
    str_list = str.split(",")

    unique_list=[]
    for item in str_list:
        if item not in unique_list:
            unique_list.append(item)

    str_list=unique_list

    len_str_list = len(str_list)

    if len_str_list == 1:
        return str
    else:
        result_list = []
        for i in range(len_str_list):
            if i <= len_str_list - 2:
                if i == 0 and str_list[i + 1].startswith(str_list[i]):
                    result_list.append(str_list[0])
                    last = str_list[i + 1]
                    diff = last[len(str_list[i]):]
                    result_list.append(diff)
                elif i != 0 and str_list[i + 1].startswith(str_list[i]):
                    last = str_list[i + 1]
                    diff = last[len(str_list[i]):]
                    result_list.append(diff)
                else:
                    return '异常'
        return "#".join(result_list)

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("ReadTXTAndSaveAsParquet") \
    .master("local[*]") \
    .getOrCreate()

# 定义 TXT 文件所在的路径，假设每个文件夹都在 /data/ 下
file_path = "file:///root/projects/spark/pyspark-example-project/data/preface.csv" 
df=spark.read.csv(file_path,header=True)
df.createOrReplaceTempView("raw_data")
preface_splited_udf=udf(preface_splited,StringType())
spark.udf.register("preface_splited",preface_splited_udf)
result=spark.sql("""
                with tmp1 as (
                select
                cast(id as int) as id,preface,case when preface is null then 1 else 0 end as tag
                from
                raw_data
                )
                , tmp2 as (
                select *,sum(tag)over(order by id) as preface_group from tmp1 order by id
                )
                , tmp3 as (
                select * ,
                concat_ws(',', collect_list(preface)over(partition by preface_group order by id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as concat_preface
                from tmp2 
                )
                 select id,preface ,preface_splited(concat_preface) as preface_splited from tmp3
""")

result.write.csv("file:///root/projects/spark/pyspark-example-project/data/preface_splited_2201" ,header=True)
spark.stop()
