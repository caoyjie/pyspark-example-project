from pyspark import SparkContext
import numpy as np

# 定义处理函数，接受两个广播变量并操作它们
def process_row(row, broadcast_array1, broadcast_array2):
    # 获取两个广播变量的值
    local_array1 = np.copy(broadcast_array1.value)  # 广播变量1的局部副本
    local_array2 = np.copy(broadcast_array2.value)  # 广播变量2的局部副本
    numbers = list(map(int, row.split(",")))  # 将字符串转换为整数
    
    # 对第一个二维数组进行操作
    for i in range(len(numbers)):
        local_array1[i // 3][i % 3] += numbers[i]  # 根据需要更新二维数组1
    
    # 对第二个二维数组进行不同的操作
    for i in range(len(numbers)):
        local_array2[i // 3][i % 3] += numbers[i] * 2  # 根据需要更新二维数组2
    
    # 返回两个结果
    return local_array1, local_array2

# 定义主函数
def main():
    # 初始化Spark上下文
    sc = SparkContext(appName="ArrayOperationWithMultipleBroadcastsAndReduce")

    # 假设我们有一个RDD，每一行是一个字符串
    data = [
        "1,2,3",
        "4,5,6",
        "7,8,9"
    ]
    rdd = sc.parallelize(data)

    # 定义两个二维数组，并将它们广播
    two_d_array1 = np.zeros((3, 3))  # 3x3的初始二维数组1
    two_d_array2 = np.ones((3, 3))   # 3x3的初始二维数组2
    broadcast_array1 = sc.broadcast(two_d_array1)
    broadcast_array2 = sc.broadcast(two_d_array2)

    # 对每一行进行操作，返回局部更新的两个二维数组
    local_arrays = rdd.map(lambda row: process_row(row, broadcast_array1, broadcast_array2))

    # 使用reduce对第一个结果（local_array1）进行聚合
    def merge_arrays1(arr1, arr2):
        return arr1 + arr2  # 使用numpy进行逐元素相加

    # 使用reduce对第二个结果（local_array2）进行不同逻辑的聚合
    def merge_arrays2(arr1, arr2):
        return np.maximum(arr1, arr2)  # 聚合时取最大值

    # 分别提取出两个局部结果
    local_array1 = local_arrays.map(lambda arrays: arrays[0])
    local_array2 = local_arrays.map(lambda arrays: arrays[1])

    # 分别使用不同的reduce聚合两个结果
    final_result1 = local_array1.reduce(merge_arrays1)  # 对第一个二维数组求和
    final_result2 = local_array2.reduce(merge_arrays2)  # 对第二个二维数组取最大值

    # 打印最终的两个二维数组
    print("Final Aggregated 2D Array 1 (Sum):")
    print(final_result1)

    print("Final Aggregated 2D Array 2 (Max):")
    print(final_result2)

    # 停止Spark上下文
    sc.stop()

# 运行主函数
if __name__ == "__main__":
    main()
