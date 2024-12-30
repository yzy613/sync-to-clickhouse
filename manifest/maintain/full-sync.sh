#!/bin/bash

# 配置变量
CLICKHOUSE_HOST="localhost"  # ClickHouse服务器地址
CLICKHOUSE_PORT="9000"       # ClickHouse服务器端口
CLICKHOUSE_USER="default"    # ClickHouse用户名
CLICKHOUSE_PASSWORD=""       # ClickHouse密码（如果没有密码，留空）
CLICKHOUSE_DATABASE="default"  # 使用的数据库
SQL_FILE="sync.sql"        # SQL文件路径
TARGET_PROCESS="sync-to-clickhouse" # 目标进程名



# 设置工作目录为脚本所在目录
cd "$(dirname "$0")" || exit 1

# 检查是否为 root 用户
if [[ $EUID -ne 0 ]]; then
  echo "Error: This script must be run as root." >&2
  exit 1
fi

# 检查sql文件是否存在
if [ ! -f "$SQL_FILE" ]; then
    echo "Error: $SQL_FILE does not exist." >&2
    exit 1
fi

# 运行SQL文件
if [ -z "$CLICKHOUSE_PASSWORD" ]; then
    clickhouse-client --host="$CLICKHOUSE_HOST" --port="$CLICKHOUSE_PORT" --user="$CLICKHOUSE_USER" --database="$CLICKHOUSE_DATABASE" --query="$(cat $SQL_FILE)"
else
    clickhouse-client --host="$CLICKHOUSE_HOST" --port="$CLICKHOUSE_PORT" --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --database="$CLICKHOUSE_DATABASE" --query="$(cat $SQL_FILE)"
fi

# 检查返回码
if [ $? -ne 0 ]; then
    echo "Error occurred while executing SQL script." >&2
    exit 1
fi

echo "SQL script executed successfully."

# 获取目标进程的PID
PID=$(pgrep -f "$TARGET_PROCESS")

# 检查是否找到进程
if [ -z "$PID" ]; then
    echo "Error: Process '$TARGET_PROCESS' not found. Unable to send SIGUSR2 signal." >&2
    exit 1
fi

echo "Found process '$TARGET_PROCESS' with PID $PID. Sending SIGUSR2 signal."
kill -SIGUSR2 "$PID"

# 检查信号是否发送成功
if [ $? -ne 0 ]; then
    echo "Error: Failed to send SIGUSR2 signal to process '$TARGET_PROCESS'." >&2
    exit 1
fi

echo "SIGUSR2 signal sent to process '$TARGET_PROCESS' successfully."
