#!/bin/bash

# 检查 MY_VAR 是否存在
if [ -z "$MY_VAR" ]; then
  echo "环境变量 MY_VAR 未设置。"
else
  echo "MY_VAR 的值是: $MY_VAR"
fi
