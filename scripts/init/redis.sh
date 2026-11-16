#!/usr/bin/env bash
# 当前框架无 Redis 业务数据；仅验证平台提供的连接、认证和逻辑库。
set -euo pipefail

: "${REDIS_HOST:?请设置 Redis 地址}"
: "${REDIS_PORT:?请设置 Redis 端口}"
: "${REDIS_DB:?请设置 Redis 逻辑库编号}"
: "${REDIS_TLS:?请设置 REDIS_TLS 为 true 或 false}"
redis_cli=${REDIS_CLI:-redis-cli}

# 1. 参数通过数组传递；密码仅由 redis-cli 读取 REDISCLI_AUTH 环境变量。
if [[ ! "$REDIS_PORT" =~ ^[1-9][0-9]*$ || ! "$REDIS_DB" =~ ^[0-9]+$ ]]; then
    echo "Redis 端口或逻辑库编号无效" >&2
    exit 1
fi
arguments=(--raw -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB")
if [[ -n "${REDIS_USER:-}" ]]; then
    arguments+=(--user "$REDIS_USER")
fi
case "$REDIS_TLS" in
    true)
        arguments+=(--tls)
        if [[ -n "${REDIS_CA_FILE:-}" ]]; then
            [[ -r "$REDIS_CA_FILE" ]] || { echo "Redis CA 文件不可读" >&2; exit 1; }
            arguments+=(--cacert "$REDIS_CA_FILE")
        fi
        ;;
    false)
        if [[ -n "${REDIS_CA_FILE:-}" ]]; then
            echo "已提供 Redis CA，请同时启用 REDIS_TLS" >&2
            exit 1
        fi
        ;;
    *) echo "REDIS_TLS 只能是 true 或 false" >&2; exit 1 ;;
esac

# 2. 校验真实响应，不创建占位 Key，不改变服务端配置或 ACL。
response=$("$redis_cli" "${arguments[@]}" PING)
if [[ "$response" != PONG ]]; then
    echo "Redis 连接或认证检查未通过" >&2
    exit 1
fi
echo "Redis 检查通过；当前框架无须预置业务 Key"
