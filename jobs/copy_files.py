import os
import shutil
from concurrent.futures import ThreadPoolExecutor

# 定义拷贝文件的函数
def copy_file(src_file, dest_file):
    try:
        # 确保是文件
        if os.path.isfile(src_file):
            shutil.copy(src_file, dest_file)  # 执行拷贝操作
        else:
            print(f"{src_file} is not a file.")
    except Exception as e:
        print(f"Error copying {src_file}: {e}")

# 定义拷贝目录的函数
def copy_directory(src_dir, dest_dir):
    # 创建目标目录，如果不存在
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
        print(f"Created directory {dest_dir}")

    # 创建线程池，设定线程数量
    with ThreadPoolExecutor(max_workers=10) as executor:
        # 遍历源目录的所有子目录和文件
        for root, dirs, files in os.walk(src_dir):
            # 计算当前目录相对于源目录的相对路径
            relative_path = os.path.relpath(root, src_dir)
            # 构造目标目录的路径
            current_dest_dir = os.path.join(dest_dir, relative_path)
            # 创建当前目标目录
            if not os.path.exists(current_dest_dir):
                os.makedirs(current_dest_dir)

            # 提交拷贝文件任务到线程池
            for file in files:
                src_file = os.path.join(root, file)
                dest_file = os.path.join(current_dest_dir, file)
                executor.submit(copy_file, src_file, dest_file)

# 源文件夹路径
source_directory = "/root/projects/spark/pyspark-example-project/data/data"
# 目标文件夹路径
destination_directory = "/root/projects/spark/pyspark-example-project/sink/data"

# 执行多线程拷贝
copy_directory(source_directory, destination_directory)
