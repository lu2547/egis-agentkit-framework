#!/usr/bin/env bash
# 首次创建唤醒、广播两个业务 Topic，重复执行时核对已有参数。
set -euo pipefail

: "${KAFKA_BOOTSTRAP_SERVERS:?请设置 Kafka broker 地址}"
: "${KAFKA_TOPIC_PREFIX:?请设置 Topic 前缀，与 Nacos 保持一致}"
: "${KAFKA_TOPIC_PARTITIONS:?请设置 Topic 分区数}"
: "${KAFKA_TOPIC_REPLICATION_FACTOR:?请设置 Topic 副本数}"
kafka_topics=${KAFKA_TOPICS_BIN:-kafka-topics.sh}

# 1. 认证参数交给平台提供的 properties 文件，不把密码拼到命令行。
if [[ ! "$KAFKA_TOPIC_PREFIX" =~ ^[a-zA-Z0-9._-]+$ \
   || ! "$KAFKA_TOPIC_PARTITIONS" =~ ^[1-9][0-9]*$ \
   || ! "$KAFKA_TOPIC_REPLICATION_FACTOR" =~ ^[1-9][0-9]*$ ]]; then
    echo "Kafka Topic 前缀、分区或副本数无效" >&2
    exit 1
fi
connection=(--bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS")
if [[ -n "${KAFKA_CLIENT_PROPERTIES:-}" ]]; then
    [[ -r "$KAFKA_CLIENT_PROPERTIES" ]] || { echo "Kafka 认证文件不可读" >&2; exit 1; }
    connection+=(--command-config "$KAFKA_CLIENT_PROPERTIES")
fi

# 2. 只建立应用的两个 Topic，不创建每个用户/Agent/Session 专属 Topic。
for suffix in triggers broadcast; do
    topic="$KAFKA_TOPIC_PREFIX.$suffix"
    "$kafka_topics" "${connection[@]}" --create --if-not-exists \
        --topic "$topic" --partitions "$KAFKA_TOPIC_PARTITIONS" \
        --replication-factor "$KAFKA_TOPIC_REPLICATION_FACTOR"

    # 3. 已有 Topic 不静默接受错误参数，也不自动扩分区或修改副本。
    # Kafka describe 将 --topic 当正则；转义点号并限定整名，避免误读相似 Topic。
    topic_pattern="^${topic//./\\.}$"
    description=$("$kafka_topics" "${connection[@]}" --describe --topic "$topic_pattern")
    shape=$(awk '{
        for (i=1; i<=NF; i++) {
            if ($i == "PartitionCount:") partitions=$(i+1)
            if ($i == "ReplicationFactor:") replicas=$(i+1)
        }
        if (partitions != "" && replicas != "") { print partitions, replicas; exit }
    }' <<< "$description")
    if [[ "$shape" != "$KAFKA_TOPIC_PARTITIONS $KAFKA_TOPIC_REPLICATION_FACTOR" ]]; then
        echo "Topic 参数不一致：${topic}；请核对分区数和副本数" >&2
        exit 1
    fi
    echo "Topic 初始化完成：$topic"
done
