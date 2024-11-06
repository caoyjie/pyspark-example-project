import subprocess

# 设置环境变量
env = {"MY_VAR": "Hello, nanjing!"}

# 使用 subprocess 运行脚本并传递环境变量
result = subprocess.run(
    ["others/print_my_var.sh"],         # 运行的脚本
    env=env,                        # 设置自定义环境变量
    capture_output=True,            # 捕获标准输出和标准错误
    text=True                       # 将输出作为字符串而非字节处理
)

# 打印脚本的输出
print("脚本输出:", result.stdout)
print("错误信息:", result.stderr)
print("returncode:", result.returncode)
""" 
脚本输出: MY_VAR 的值是: Hello, nanjing!

错误信息: 
"""
