# EgisMessageBus 实现与验收

实现目录：`src/egis_agentkit/message_bus/`。继承原生 `agentscope.app.message_bus.MessageBus`，实现全部抽象方法；业务 key 直接使用原生 `MessageBusKeys`。未改动原生 Dispatcher、ChatService、AgentLoop 或原生进程内 `ChatRunRegistry`。

## 接入与生命周期

```python
from egis_agentkit.message_bus import EgisMessageBus

bus = EgisMessageBus(
    deployment_mode="cluster",             # 默认 standalone
    engine=shared_async_engine,              # 调用方拥有的 SQLAlchemy AsyncEngine
    kafka_bootstrap_servers="localhost:9092",
    topic_prefix="egis-agentkit",
    consumer_group="egis-agentkit-wakeup-v1", # 同一集群各节点必须一致
    node_id="app-node-1",
    buffer_size=256,
    kafka_options={},                        # TLS/SASL 等服务端连接选项
)
await bus.migrate()                          # 显式、幂等、仅增加 PG 表/索引
await bus.ensure_topics(partitions=8, replication_factor=1)  # 可由运维预创建
async with bus:
    # 交给原生 create_app / lifespan 使用；生产配置由上层 Nacos 装配。
    ...
```

`migrate()` 使用事务 advisory lock 协调多实例并发迁移，不自动执行在启动路径中。`ensure_topics()` 只创建不存在的 topic，不修改现有 topic 的分区、保留时间或副本参数。生产环境可以不给运行账号 topic 创建权限，由发布流程预创建。两者都不调用 `engine.dispose()`。`aclose()` 停止本实例的 Kafka 客户端、后台任务和订阅，取消仍持租约的其他任务，尽力按 owner token 释放锁；调用方负责最终关闭共享 engine。关闭后的 bus 不可重新进入。

`bus.health()` 是同步方法，返回 `is_ready`、`deployment_mode`、`is_closed`；cluster 另带 `kafka` 状态 flags。该快照检查 producer sender、两个 receiver task、consumer 生命周期、订阅就绪和后台 fatal 状态；不返回异常文本、连接配置或凭据。fatal receiver、停止的 producer、关闭中的 bus 都不能保持 ready。它不是 PG/Kafka 网络探针，上层健康端点应结合存储/网络探测。

`standalone` 完全委托原生 `InMemoryMessageBus`。因此仅支持单进程，原生内存 log/registry 的 TTL 限制也保留；不能把本实现的 cluster TTL 保证套到 standalone。无需 PG 或 aiokafka 即可使用 standalone。

## 原生语义对应

| 原语 | cluster 实现 | 消费/生命周期边界 |
| --- | --- | --- |
| 普通 `queue_push/drain/delete` | PG keyed queue | `DELETE ... RETURNING` + `FOR UPDATE SKIP LOCKED`，事务提交后返回；并发 drain 不重叠 |
| `log_append/read/trim` | PG replay log | 非破坏读取；`since` 严格排除；保留每 key 的序号墓碑，trim/TTL 后序号仍递增 |
| `registry_*` | PG namespace + field/value | CAS、pop 均为事务原子操作；TTL 属于整个 namespace |
| `acquire_lock` | PG token lease + 心跳 | 每 TTL/3 续期；丢失所有权/续期超时即取消持有任务并报告 `LeaseLostError` |
| `try_lock/unlock` | PG token lease | 非阻塞、无自动续期；只有获取锁的同一个 bus/asyncio task 能释放其 token |
| 全局 wakeup queue | Kafka `<prefix>.triggers` | 所有节点使用一个 consumer group；本节点仅 drain 本消费者已分配并已缓冲的记录 |
| wakeup signal | 本地 level 通知 | `queue_push(wakeup_queue)` 本身生产 Kafka 记录；兼容的 `publish(wakeup_signal)` 为 no-op，不向其他节点广播抢队列 |
| 其他任意 `publish/subscribe` key | Kafka `<prefix>.broadcast` | 每节点独立 group，向本节点该 key 的所有当前订阅者扇出；包括 SSE、cancel、index 和自定义 key |

PG 表为 `egis_bus_keys`、`egis_bus_entries`、`egis_bus_registry`、`egis_bus_leases`、`egis_bus_broadcast_payloads`。queue/log/registry 各自有独立 key 空间。log 的同 key append 通过 PG 行锁串行提交；不会出现先分配小序号但后提交、导致读者越过未提交记录的序列漏洞。cursor 为 `<integer>-0`，非法 cursor 明确报错。队列与 log 的返回顺序按各自 key 的序号排列；并发 SKIP LOCKED drain 可以分别取得不同批次，不提供跨调用的完成顺序。

cluster 的正数 `ttl_secs` 在每次写入时刷新整个 key 的到期时间；省略 TTL 表示取消该 key 的到期限制。到期数据读取立即不可见，后台每 60 秒清理至多 500 个有残留数据的到期 key，避免已清空墓碑反复占满清理批次。序号墓碑保留，以便旧 cursor 继续有效，因此元数据大小随曾使用过的 key 数增长。

wakeup 的 TTL/delete 用 PG 中按 topic 隔离的 generation 元数据实现。逻辑删除后，旧 generation 的 Kafka 记录不会重新触发执行；物理 Kafka 字节由 topic 保留策略回收。producer 的 session_id 用作分区 key，只保证该 Kafka 分区内顺序，不保证所有会话之间的全局顺序。

## Kafka 缓冲、订阅与 rebalance

消费者 `enable_auto_commit=False`。本地 pending 队列最多 `buffer_size` 条；满时暂停分区但继续 poll 保持 group 活跃，空间释放后恢复。Kafka 自身的 fetch 缓存另有大小上限，producer 使用 aiokafka 默认单请求大小限制。每个广播订阅也有有限队列，慢订阅者对本节点的广播泵施加背压；不通过无限内存队列掩盖慢消费者。

广播序列化 envelope 加 key/记录开销超过 512 KiB 时，先将完整原始 dict 提交到 `egis_bus_broadcast_payloads`，再发送 `{key,payload_ref,version:1}` 小记录。小广播继续使用原来的 `{key,payload}`；没有提高 producer/broker/topic 的大小上限。PG body 使用不可预测 UUID，读取必须同时匹配广播 topic namespace、key 和引用，不能跨 namespace/key 取数。接收泵按 Kafka offset 顺序取回 dict，一次读取供本节点全部当前订阅者共享；订阅起始 offset 屏障和原 payload（包括 `_entry_id`）不变。本实例同 key 的大/小并发 publish 排队，防止 PG 提交期间后发小事件超车；不同 key 独立，不因大图片写 PG 阻塞取消通道。跨实例仍以 Kafka 分区 offset 定序，不提供全局调用顺序。

单条广播 envelope 上限 32 MiB，UTF-8 key 上限 64 KiB，超限在写 PG/Kafka 前明确报错；32 MiB 可容纳 20 MiB 图片的 base64 结果及事件元数据。body TTL 为 1 小时，PG 写入/读取各限时 30 秒。后台每 60 秒另清理最多 500 个到期 body（`SKIP LOCKED`），不保留序号墓碑。进程在 PG 提交后、Kafka 发送前退出，或 Kafka ACK 丢失时，可能留下 body，均由 TTL 清理；发送异常不提前删除可能已被 broker 接受的引用。PG/Kafka 之间没有新建分布式事务或 outbox。数据库容量仍需按一小时内的大事件速率规划；清理吞吐不足时需运维增加清理频率，不能把 TTL 描述为数据库字节数硬配额。订阅队列按条数有限，最大事件增加后应结合单条上限设置 `buffer_size`。

body 与公开 session replay log 独立，`log_trim`/`max_len` 不使已发送的引用失效。已有 `log_append/read/trim` 的内容、cursor、TTL 语义不变，native `publish_session_event` 仍先写完整 replay 再广播。缺失/过期 body 抛出可监控的 `BroadcastPayloadUnavailableError`，读取异常/超时结束接收泵、令 health 不就绪并通知现有订阅者，不静默跳过事件；原生 session SSE 可在修复/重连后按 replay cursor 读取尚保留的完整事件。任意自定义广播没有自动补发，超过一小时背压仍可能令该瞬时通道失败。未调整 wakeup trigger 单条记录大小，其持久队列语义不使用有 TTL 的广播 body。

部署时先显式执行 `bus.migrate()` 增加 body 表及到期索引，再升级所有广播消费者。旧版本消费者不识别 `payload_ref`，新旧读取版本不应混用。无需修改 Nacos、Kafka topic 参数或 AgentScope 服务；不需要将 body 写入业务 storage migration。数据库异常只向上传递固定文案和异常类型，避免 SQL 参数中的 base64/连接信息出现在原生日志中。

wakeup `on_ready` 在消费者已经建立 group 订阅后调用一次。初始 drain 可以暂时没有记录，后续 fetch 会设置本地通知；仍有 pending 记录时会继续设置通知，原生每次只 drain 64 条也不会把剩余记录搁置。其他 key 的订阅先获取 broker end offsets 作为起始屏障，再调用一次 `on_ready`，不发送订阅前的历史事件；关闭生成器会立即关闭内层订阅并释放背压等待。

`queue_drain` 只提交本次 batch 所覆盖的每分区最后 offset + 1，不提交 `consumer.position()`。后者可能已经包含预取但尚未 drain 的记录。提交失败保留 pending 数据并唤醒下一次 drain。分区 revoke 与 drain 共用本地锁：未 drain 的记录不提交，交给新 owner 从已提交 offset 重读；跨 rebalance 完成的 fetch 会重新定位，不能简单丢弃已推进消费位置的 fetch 结果。

aiokafka 执行底层重连；可重试的 receive 错误延迟后继续读取。不可恢复的后台异常记录错误类型，并传递给当前订阅者及后续 Kafka 操作。原生 Dispatcher 会记录其订阅异常并结束，修复配置或权限后应重启应用实例；本实现不复制一个新的业务 Dispatcher 监督循环。损坏的 trigger envelope 不会被静默 ACK，需运维处理该记录；当前没有自动 DLQ。瞬时广播的损坏记录会记录 partition/offset 后跳过。

## ACK 与锁的实际保证

**本实现遵守原生 ack-on-read，不能宣称业务 exactly-once，也不能宣称运行完成后才 ACK。**

1. 普通 PG queue 的读取与删除在同一事务内完成；返回后进程退出会丢失该批已经读取的数据。
2. Kafka trigger 在 `queue_drain` 内先提交选中记录的 offsets，再返回 payload。若此后、甚至返回前进程崩溃，已提交的记录不会由 group 自动重投。
3. 原生 `WakeupDispatcher` drain 后逐条校验并调用 `ChatRunRegistry.spawn`，**不等待 ChatService.run 完成**。因此从 drain 到校验、spawn、原生重试定时器重新入队、实际运行完成之间，都不是 Kafka 的业务 ACK 边界。一次 drain 返回的整批都有这个窗口。
4. broker 已接受 commit 但响应丢失时，客户端无法从异常证明 commit 没发生；继续重试或崩溃重启可能出现不确定性。producer 幂等发送也不等于业务执行幂等。
5. PG lease 的 token/CAS 防止旧 owner 解锁或续租新 owner 的锁。续期失败会取消协程，清理释放也有超时，避免数据库故障把 `LeaseLostError` 一直藏在清理等待中；最终依靠租约到期恢复可用性。
6. lease 没有向外部系统传递 fencing token。进程长时间暂停、事件循环阻塞、协程吞掉取消，以及已发出的外部请求，都不能仅靠本 API 保证停止副作用。需要业务幂等或接收端 fencing 的场景必须在相应业务边界实现。
7. `log_append` 与随后 `publish` 是两个动作；发送 live 通知失败时，log 仍可供原生重连/replay 读取。广播属于瞬时 best-effort，不保证离线或故障期间事件补发。

topic retention、Kafka group offset retention、副本数和 `min.insync.replicas` 是运维配置的一部分。`ensure_topics` 沿用 broker topic 默认值；若配置有限 retention，超过该窗口的离线 trigger 会被 Kafka 清理，不能把它描述为无限保留队列。生产需按可接受停机窗口设置这些值，或选用无限 retention 并规划存储回收。此版本未提供业务 run ACK/outbox 协议，不应借文档措辞扩大接口能力。

## 验收方式和证据

环境：本机 PostgreSQL `localhost:5433`、Kafka 3.8.1 `localhost:9092`、AgentScope 2.0.11、aiokafka 0.15.0。凭据由测试进程读取 `deploy/agentkit-infra/pg.env`；不写入本文件。PG 测试各自使用随机临时 schema，结束后清理；Kafka 测试各自创建随机 topic，结束后删除。测试没有启动 Redis。

在 workspace 根目录执行：

```bash
EGIS_BUS_INTEGRATION=1 PYTHONPATH=egis/egis-agentkit-framework/src \
  demo/agentscope-demo/.venv/bin/python -m pytest \
  egis/egis-agentkit-framework/tests/test_message_bus.py \
  egis/egis-agentkit-framework/tests/test_message_bus_kafka.py \
  egis/egis-agentkit-framework/tests/test_message_bus_large_payloads.py \
  -q -o asyncio_default_fixture_loop_scope=function
```

没有 `EGIS_BUS_INTEGRATION=1` 时，只有纯 standalone/构造验证运行，其余测试明确 skip。`test_message_bus_large_payloads.py` 使用真实临时 PG schema 和拒绝超过 1 MiB 记录的有限 broker 测试替身，实际运行公开 adapter、native event helper 和接收泵，还原完整大图片事件而非 mock 成功。真实两节点 Kafka/PG 测试另覆盖 28 MiB 图片事件、同 key 并发小事件顺序与原始 replay 内容。

已验证的重点：

- 原生继承兼容及 standalone 委托，订阅即时关闭；PG 90 条并发 push/drain 不重复，log 并发提交/严格 cursor/trim，整个 key 的滑动 TTL，registry CAS/pop 竞争，过期清理跨批不饥饿。
- lease owner-only unlock、过期后旧 owner 不释放新 owner、长运行续租、所有权丢失取消、嵌套 lease 退出后仍能在关闭时取消外层任务、数据库释放连接挂起时仍及时报告 lease 丢失；过期 claim 不强引用已经完成的 owner task。
- 两个独立 bus 实例使用真实 Kafka/PG：90 个触发器由一个共享 group 分配给两节点，稳定运行时每条仅被一个 drain 取走；取消、SSE、索引和任意公开 key 都能广播到两节点且不重放历史。
- buffer_size=5 时节点未 drain 就退出，另一节点 rebalance 后可取全 40 条；人工注入 commit 失败后仍可 drain 原记录，已提交 offset 不覆盖同分区尚未 drain 的第二条记录。
- wakeup TTL/delete 后旧记录不复活；直接使用未改动的原生 WakeupDispatcher、CancelDispatcher、ChatRunRegistry 验证跨节点 spawn 与取消；不可恢复的接收异常能传递到订阅者；health 同步快照在 standalone 生命周期、fatal receiver 和 producer 停止时正确改变 readiness。

两个 bus 实例运行在同一 pytest 进程，使用独立 Kafka consumer/group 成员及真实 PG 事务。测试中的 ChatService.run 仅用一个协程替身验证原生 Dispatcher/Registry 边界，不代表真实模型、HTTP/SSE 字节流或业务工具成功。已测优雅退出后的 rebalance 和人工故障注入；未把它夸大为 OS 级 SIGKILL、Kafka broker 重启或网络分区测试。

执行记录：2026-11-04，以上两个 bus 测试文件合并执行，最终结果 **`24 passed in 115.99s`**（16 个 PG/standalone/lifecycle 测试，8 个真实 Kafka/PG 测试）。同版本源码的 `python -m compileall -q egis/egis-agentkit-framework/src/egis_agentkit/message_bus` 返回 0。

执行记录：2026-11-05，加入大事件回归后的三个 bus 测试文件合并执行，结果 **`34 passed in 129.07s`**。包含 9 个有限 broker/真实 PG 大事件与故障回归，以及新增的真实两节点 28 MiB 图片和并发小事件顺序测试。原超过 1 MiB 的回归先观察到 `MessageSizeTooLargeError` 再修复通过；并发大/小事件超车和跨 key 控制通道阻塞也分别先复现失败，再修复通过。上述四个本次修改的 Python 文件通过 Ruff lint，bus 源码及测试 `compileall` 返回 0。

framework 全量由集成根任务统一验收；本 worker 的全量尝试曾在 `tests/test_assembly.py` 收集阶段遇到当时尚未生成的 `egis_agentkit.services.assembly`，随后根任务已继续实现该模块，本文不宣称该次全量通过。
