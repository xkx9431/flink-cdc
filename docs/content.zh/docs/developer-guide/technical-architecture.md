---
title: "技术架构分析"
weight: 3
type: docs
aliases:
  - /developer-guide/technical-architecture
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Flink CDC 技术架构分析

本文深入分析 Flink CDC 的核心技术设计，重点介绍三个关键方面：**插件化机制**、**数据一致性**和**高可用性**。

## 概述

Flink CDC 构建在 Apache Flink DataStream API 之上。一个 Pipeline 任务在运行时将 YAML 定义翻译为 Flink 作业图。
下图展示了高层次的组件布局：

```
YAML Pipeline 定义
        │
        ▼
  PipelineComposer（流水线编译器）
        │  翻译为
        ▼
  DataStream<Event>
  ┌──────────────────────────────────────────────┐
  │  Source ──► Transform ──► Schema ──► Sink    │
  │  (事件源)  (算子)     (Schema算子) (写入算子) │
  └──────────────────────────────────────────────┘
        │  在 Flink 集群上执行
        ▼
  Apache Flink 集群
```

四个核心模块分工如下：

| 模块 | 职责 |
|---|---|
| `flink-cdc-common` | 共享接口、事件模型、数据类型 |
| `flink-cdc-runtime` | Flink 算子、Schema 协调、序列化 |
| `flink-cdc-composer` | YAML 到 Flink 作业图的翻译 |
| `flink-cdc-connect` | Source 和 Sink 连接器实现 |

---

## 插件化机制

Flink CDC 通过 Java 的 **服务提供者接口（SPI）** 机制结合 **工厂模式（Factory Pattern）** 实现连接器插件化。
这允许在不修改核心框架的情况下添加或替换连接器。

### 工厂接口设计

所有连接器实现 `flink-cdc-common` 中定义的工厂接口之一：

```
Factory（基础接口）
├── DataSourceFactory   – 创建 DataSource 实例
└── DataSinkFactory     – 创建 DataSink 实例
```

每个工厂通过 `identifier()` 声明其唯一标识，并通过 `requiredOptions()` 和 `optionalOptions()`
声明必填和可选的配置项。`FactoryHelper` 工具类在 Pipeline 启动时负责验证连接器配置的合法性。

### SPI 注册

每个连接器 JAR 包通过以下资源文件注册其工厂实现：

```
META-INF/services/org.apache.flink.cdc.common.factories.Factory
```

文件内容为工厂实现类的全限定类名，例如：

```
org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory
```

### 运行时发现

`FactoryDiscoveryUtils` 使用 `ServiceLoader` 发现 Classpath 上的所有工厂：

```java
ServiceLoader<Factory> loader = ServiceLoader.load(Factory.class, classLoader);
// 根据 identifier 匹配工厂，验证唯一性
Factory factory = getFactoryByIdentifier(identifier, DataSourceFactory.class);
```

提交 Pipeline 时，CLI 工具将连接器 JAR 放置在 Flink 的 `lib` 目录中（或通过 `--jar` 参数传入）。框架根据
YAML 配置中的 `type` 字段匹配正确的工厂。

### 连接器接口分层

每个连接器实现清晰的双层接口：

**Source 侧：**

| 接口 | 职责 |
|---|---|
| `DataSource` | 工厂创建的对象；提供 `EventSource` 和 `MetadataAccessor` |
| `EventSource` | Flink Source，读取 CDC 事件并向下游发送 |
| `MetadataAccessor` | 从源数据库读取表结构、命名空间和表列表 |

**Sink 侧：**

| 接口 | 职责 |
|---|---|
| `DataSink` | 工厂创建的对象；提供 `EventSinkProvider` 和 `MetadataApplier` |
| `EventSink` | Flink Sink，接收 `DataChangeEvent` 并写入目标系统 |
| `MetadataApplier` | 将 `SchemaChangeEvent`（DDL）应用到目标系统 |

这种双接口设计清晰地分离了**数据路径**（高吞吐量 DML）和**控制路径**（低频 DDL）。

### 连接器模块结构

```
flink-cdc-connect/
├── flink-cdc-source-connectors/       （Flink Table/SQL API 连接器）
│   ├── flink-connector-mysql-cdc/
│   ├── flink-connector-postgres-cdc/
│   └── ...
└── flink-cdc-pipeline-connectors/     （Pipeline API 连接器）
    ├── flink-cdc-pipeline-connector-mysql/
    ├── flink-cdc-pipeline-connector-kafka/
    ├── flink-cdc-pipeline-connector-doris/
    └── ...
```

添加一个新连接器只需实现上述接口并通过 SPI 注册工厂，**无需对核心框架做任何修改**。

---

## 数据一致性保障

Flink CDC 通过 Flink Checkpoint、有序事件流和基于 Flush 的 Schema 变更协调协议，提供**端到端精确一次（Exactly-Once）数据一致性**。

### 事件流有序性约定

框架对事件流强制执行严格的有序规则：

1. 对于新表，`CreateTableEvent` **必须**先于任何 `DataChangeEvent` 发出。
2. 对于 Schema 变更，`SchemaChangeEvent` **必须**先于依赖新 Schema 的 `DataChangeEvent` 发出。

这保证了每个算子在处理数据记录之前，始终能获取到最新的 Schema 信息。
Schema 信息由 `SchemaOperator` 内部的 `SchemaManager` 负责维护。

### FlushEvent 屏障协议（Schema 变更一致性）

当 Source 发出 `SchemaChangeEvent`（如 `AddColumnEvent`）时，框架必须确保所有携带**旧 Schema** 的
在途 `DataChangeEvent` 都已写入 Sink，然后才能应用 Schema 变更。这通过 `FlushEvent` 屏障协议实现：

```
Source 发出 SchemaChangeEvent
        │
        ▼
SchemaOperator 接收到变更事件
        │
        ▼  （1）向所有 Sink Writer 子任务广播 FlushEvent
        │
        ▼  （2）每个 DataSinkWriterOperator 刷新待写数据，
        │       并向 SchemaOperator 回复 FlushSuccessEvent
        │
        ▼  （3）SchemaOperator 等待所有 Sink Writer 确认完成
        │
        ▼  （4）通过 MetadataApplier 向 Sink 应用 SchemaChangeEvent
        │
        ▼  （5）以新 Schema 恢复正常数据流
```

`FlushEvent` 携带 Source 子任务 ID 和需要 Flush 的表 ID 列表，在多表 Pipeline 中支持按需选择性 Flush。

### Checkpoint 集成

Flink CDC 与 Flink 标准 Checkpoint 机制深度集成：

- `DataSinkWriterOperator` 实现 `snapshotState` 和 `initializeState`，在 Checkpoint 时捕获并恢复写入状态。
- `prepareSnapshotPreBarrier` 用于在 Checkpoint Barrier 通过前刷新缓冲数据。
- `SchemaOperator`（及其协调器）将完整的 Schema 注册表状态序列化到 Checkpoint，
  确保 Schema 版本映射在故障恢复后完整还原。

Checkpoint 与 `FlushEvent` 屏障共同保证：在重启后，数据记录和 Schema 变更均不会丢失或重复。

### Schema 变更行为配置

对 Sink 应用 Schema 变更时的行为可通过 Pipeline 选项 `schema.change.behavior` 进行配置：

| 模式 | 行为 |
|---|---|
| `exception` | 拒绝所有 Schema 变更，抛出异常 |
| `evolve` | 应用 Schema 变更；出错时抛出异常 |
| `try_evolve` | 应用 Schema 变更；对不支持的类型容忍失败 |
| `lenient`（默认）| 将不兼容的变更转换为安全等价形式（如类型变更 → 重命名列 + 新增列） |
| `ignore` | 静默丢弃所有 Schema 变更 |

还可以通过 `sink` 块中的 `include.schema.changes` 和 `exclude.schema.changes` 对各类 Schema 变更事件进行细粒度控制。

---

## 高可用性保障

Flink CDC 利用 Flink 的 `OperatorCoordinator` API 实现了一个支持容错的分布式 Schema 协调服务。

### SchemaRegistry 作为 OperatorCoordinator

`SchemaRegistry`（及其分布式子类 `SchemaCoordinator`）作为 Flink `OperatorCoordinator` 运行，这意味着：

- 它运行在 **JobManager** 进程中，独立于 TaskExecutor。
- 它通过 `CoordinationRequest` / `CoordinationResponse` 消息与算子通信。
- 其状态包含在 Flink Checkpoint 中，可完整恢复。

```
JobManager
└── SchemaRegistry（OperatorCoordinator）
        │  CoordinationRequest/Response
        ├── SchemaOperator 子任务 0
        ├── SchemaOperator 子任务 1
        └── DataSinkWriterOperator 子任务 N
```

### 故障恢复协议

当某个子任务发生故障并重启时，通过以下流程保证一致性：

1. 每个 `DataSinkWriterOperator` 在启动时发送 `SinkWriterRegisterEvent`，向协调器注册自身。
2. 协调器维护活跃 Sink Writer 的集合。
3. 在 Schema 变更协调过程中，协调器等待**所有**已注册 Sink Writer 发送 `FlushSuccessEvent` 后才继续。
4. 若某子任务在协调过程中重启，则重新注册后，协调器重新发送进行中的 `FlushEvent`，子任务再次 Flush。

该协议保证：只有在所有 Sink Writer 均已确认先前数据已持久化写入之后，Schema 变更才会被应用。

### 分布式 Source 去重

当 Source 是并行的（如 Kafka 或 MongoDB 的多个分区），同一个 `SchemaChangeEvent` 可能被多个 Source
子任务发出。`SchemaCoordinator` 通过每个（分区, 事件）对的缓存进行去重：

```java
// SchemaCoordinator
alreadyHandledSchemaChangeEvents.get(Tuple2<SourcePartitionId, SchemaChangeEvent>)
```

只有第一次出现时才触发实际的 Schema 变更请求；后续重复的事件立即被确认，不做处理。

### 稳定的算子 UID

Flink 的状态恢复要求算子 UID 在作业重启前后保持稳定。Flink CDC 通过 `pipeline.operator.uid.prefix`
选项为所有算子自动分配确定性 UID，确保 Checkpoint 中的状态（包括 Schema 映射表和 Sink 写入状态）在重启或
升级后能够正确映射到对应算子。

---

## 总结

| 关注点 | 实现机制 |
|---|---|
| **插件化** | 基于 SPI 的 `Factory` 发现机制；连接器为独立自包含的 JAR 包 |
| **事件有序性** | `CreateTableEvent`/`SchemaChangeEvent` 必须先于对应的 `DataChangeEvent` |
| **Schema 一致性** | `FlushEvent` 屏障在 DDL 应用前同步所有 Sink Writer |
| **Checkpoint 安全性** | Schema 注册表状态与写入状态随每次 Checkpoint 一同快照 |
| **故障容错** | 基于 `OperatorCoordinator` 的协调器；子任务重启后重新注册 |
| **分布式去重** | `SchemaCoordinator` 对并行 Source 分区的 Schema 事件去重 |
| **稳定恢复** | 通过 `pipeline.operator.uid.prefix` 为算子分配确定性 UID |
