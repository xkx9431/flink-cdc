---
title: "Technical Architecture"
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

# Technical Architecture of Flink CDC

This document provides an in-depth analysis of the core technical design of Flink CDC, focusing on three key
aspects: **pluggability**, **data consistency**, and **high availability**.

## Overview

Flink CDC is built on top of Apache Flink's DataStream API. A pipeline job translates a YAML definition
into a Flink job graph at runtime. The following diagram illustrates the high-level component layout:

```
YAML Pipeline Definition
        │
        ▼
  PipelineComposer
        │  translates to
        ▼
  DataStream<Event>
  ┌──────────────────────────────────────────────┐
  │  Source ──► Transform ──► Schema ──► Sink    │
  │  (EventSource)  (Operators) (Operator) (EventSink) │
  └──────────────────────────────────────────────┘
        │  executes on
        ▼
  Apache Flink Cluster
```

The four core modules are:

| Module | Responsibility |
|---|---|
| `flink-cdc-common` | Shared interfaces, event model, data types |
| `flink-cdc-runtime` | Flink operators, schema coordination, serialization |
| `flink-cdc-composer` | YAML → Flink job graph translation |
| `flink-cdc-connect` | Source and sink connector implementations |

---

## Pluggability

Flink CDC achieves connector pluggability through Java's **Service Provider Interface (SPI)** mechanism combined
with a **Factory pattern**. This allows connectors to be added or replaced without modifying the core framework.

### Factory Interfaces

All connectors implement one of these factory interfaces defined in `flink-cdc-common`:

```
Factory (base interface)
├── DataSourceFactory   – creates DataSource instances
└── DataSinkFactory     – creates DataSink instances
```

Each factory declares its unique `identifier()` and the required/optional configuration options via
`requiredOptions()` and `optionalOptions()`. The `FactoryHelper` utility validates connector configuration
at pipeline startup.

### SPI Registration

Each connector JAR registers its factory in its resources:

```
META-INF/services/org.apache.flink.cdc.common.factories.Factory
```

The file content is the fully-qualified class name of the factory implementation, for example:

```
org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory
```

### Discovery at Runtime

`FactoryDiscoveryUtils` uses `ServiceLoader` to discover all factories on the classpath:

```java
ServiceLoader<Factory> loader = ServiceLoader.load(Factory.class, classLoader);
// Matches factory by identifier, validates uniqueness
Factory factory = getFactoryByIdentifier(identifier, DataSourceFactory.class);
```

When a pipeline is submitted, the CLI places connector JARs in the Flink `lib` directory (or passes them via
`--jar`). The framework then resolves the correct factory by the `type` field in the YAML configuration.

### Connector Interfaces

Each connector implements a clean two-layer interface:

**Source side:**

| Interface | Role |
|---|---|
| `DataSource` | Factory-created object; provides `EventSource` and `MetadataAccessor` |
| `EventSource` | Flink source that reads CDC events and emits them downstream |
| `MetadataAccessor` | Reads table schemas, namespaces, and table lists from the source database |

**Sink side:**

| Interface | Role |
|---|---|
| `DataSink` | Factory-created object; provides `EventSinkProvider` and `MetadataApplier` |
| `EventSink` | Flink sink that receives `DataChangeEvent` and writes to the target system |
| `MetadataApplier` | Applies `SchemaChangeEvent` (DDL) to the target system |

This two-interface design cleanly separates data path (high-throughput DML) from control path (low-frequency DDL).

### Connector Module Layout

```
flink-cdc-connect/
├── flink-cdc-source-connectors/       (Flink Table/SQL API connectors)
│   ├── flink-connector-mysql-cdc/
│   ├── flink-connector-postgres-cdc/
│   └── ...
└── flink-cdc-pipeline-connectors/     (Pipeline API connectors)
    ├── flink-cdc-pipeline-connector-mysql/
    ├── flink-cdc-pipeline-connector-kafka/
    ├── flink-cdc-pipeline-connector-doris/
    └── ...
```

Adding a new connector only requires implementing the above interfaces and registering the factory via SPI —
no changes to the core framework are needed.

---

## Data Consistency

Flink CDC provides **end-to-end exactly-once data consistency** through a combination of Flink checkpoints,
ordered event streams, and a flush-based schema change coordination protocol.

### Event Ordering Contract

The framework enforces a strict ordering rule on the event stream:

1. A `CreateTableEvent` **must** precede any `DataChangeEvent` for a new table.
2. A `SchemaChangeEvent` **must** precede any `DataChangeEvent` that depends on the new schema.

This means every operator can safely assume that an up-to-date schema is always available before it processes
any data record. Schema information is maintained in a `SchemaManager` inside the `SchemaOperator`.

### FlushEvent Barrier for Schema Changes

When a source emits a `SchemaChangeEvent` (e.g., `AddColumnEvent`), the framework must ensure that all
in-flight `DataChangeEvent` records carrying the *old* schema have been written to the sink before the schema
change is applied. This is achieved through the `FlushEvent` barrier protocol:

```
Source emits SchemaChangeEvent
        │
        ▼
SchemaOperator receives it
        │
        ▼  (1) Broadcast FlushEvent to all sink writer subtasks
        │
        ▼  (2) Each DataSinkWriterOperator flushes pending writes
        │       and sends FlushSuccessEvent back to SchemaOperator
        │
        ▼  (3) SchemaOperator waits until ALL sink writers confirm
        │
        ▼  (4) Apply SchemaChangeEvent to sink via MetadataApplier
        │
        ▼  (5) Resume normal data flow with new schema
```

The `FlushEvent` carries the source subtask ID and the list of table IDs that need flushing, enabling
selective flushing in multi-table pipelines.

### Checkpoint Integration

Flink CDC integrates with Flink's standard checkpoint mechanism:

- `DataSinkWriterOperator` implements `snapshotState` and `initializeState` to capture and restore write state.
- `prepareSnapshotPreBarrier` is used to flush buffered data before a checkpoint barrier passes.
- The `SchemaOperator` (and its coordinator) also serializes the full schema registry state into checkpoints,
  so the schema version map is fully recoverable after a failover.

Together, checkpoints and `FlushEvent` barriers ensure that neither data records nor schema changes are
duplicated or lost across restarts.

### Schema Change Behaviors

The behavior when applying schema changes to the sink is configurable via the `schema.change.behavior`
pipeline option:

| Mode | Behavior |
|---|---|
| `exception` | Reject all schema changes; throw an exception |
| `evolve` | Apply schema changes; fail on error |
| `try_evolve` | Apply schema changes; tolerate unsupported types |
| `lenient` (default) | Convert incompatible changes to safe equivalents (e.g., type change → rename + add column) |
| `ignore` | Silently drop all schema changes |

Per-event-type inclusion and exclusion can be further controlled with `include.schema.changes` and
`exclude.schema.changes` in the `sink` block.

---

## High Availability

Flink CDC leverages Flink's `OperatorCoordinator` API to implement a fault-tolerant, distributed schema
coordination service.

### SchemaRegistry as OperatorCoordinator

The `SchemaRegistry` (and its distributed subclass `SchemaCoordinator`) runs as a Flink
`OperatorCoordinator`, which means:

- It runs in the **JobManager** process, outside the task executor.
- It communicates with operators via `CoordinationRequest` / `CoordinationResponse` messages.
- Its state is included in Flink checkpoints and is fully recoverable.

```
JobManager
└── SchemaRegistry (OperatorCoordinator)
        │  CoordinationRequest/Response
        ├── SchemaOperator subtask 0
        ├── SchemaOperator subtask 1
        └── DataSinkWriterOperator subtask N
```

### Failover Protocol

When a subtask fails and restarts, it re-registers with the coordinator:

1. Each `DataSinkWriterOperator` sends a `SinkWriterRegisterEvent` on startup.
2. The coordinator tracks the set of active sink writers.
3. During schema change coordination, the coordinator waits until **all** registered sink writers have
   sent `FlushSuccessEvent` before proceeding.
4. If a subtask restarts mid-coordination, it re-registers, the coordinator re-sends the in-progress
   `FlushEvent`, and the subtask flushes again.

This protocol guarantees that a schema change is never applied until every sink writer has confirmed that all
prior data is durably written.

### Distributed Source Deduplication

When sources are parallel (e.g., Kafka or MongoDB with multiple partitions), the same `SchemaChangeEvent`
may be emitted by multiple source subtasks. The `SchemaCoordinator` deduplicates these events using a
per-(partition, event) cache:

```java
// SchemaCoordinator
alreadyHandledSchemaChangeEvents.get(Tuple2<SourcePartitionId, SchemaChangeEvent>)
```

Only the first occurrence triggers an actual schema change request; subsequent duplicates are acknowledged
immediately.

### Stable Operator UIDs

Flink's state recovery requires that operator UIDs are stable across job restarts. Flink CDC automatically
assigns deterministic UIDs to all operators using the `pipeline.operator.uid.prefix` option. This ensures
that checkpointed state (including schema maps and sink write state) is correctly mapped to operators after
a restart or upgrade.

---

## Summary

| Concern | Mechanism |
|---|---|
| **Pluggability** | SPI-based `Factory` discovery; connectors are self-contained JARs |
| **Event Ordering** | `CreateTableEvent`/`SchemaChangeEvent` must precede `DataChangeEvent` |
| **Schema Consistency** | `FlushEvent` barrier synchronizes all sink writers before DDL is applied |
| **Checkpoint Safety** | Schema registry state and write state are snapshotted with each checkpoint |
| **Fault Tolerance** | `OperatorCoordinator`-based coordinator; subtask re-registration on restart |
| **Distributed Dedup** | `SchemaCoordinator` deduplicates schema events across parallel source partitions |
| **Stable Recovery** | Deterministic operator UIDs via `pipeline.operator.uid.prefix` |
