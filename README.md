# 📨 Reactive Messenger

> 🌍 Other languages:
> - [Russian](./README.ru.md)
> - [English](./README.md)

## 📘 Contents

- [📨 Reactive Messenger — Overview](#-reactive-messenger)
- [🚀 Key Idea](#-key-idea)
- [🧩 Core Technologies](#-core-technologies)
- [🧭 Project Ideology](#-project-ideology)
- [⚙️ System Core — State Machine (Pipeline)](#-1-system-core--state-machine-pipeline)
- [⚡️ Reactive Model and Load Management](#-2-reactive-model-and-load-management)
- [💳 User Credit Mechanism](#-3-user-credit-mechanism)
- [🧱 Architectural Approach — Hexagonal Architecture](#-4-architectural-approach--hexagonal-architecture)
- [🧾 Guarantees and Goals](#-5-guarantees-and-goals)
- [💬 Message Types and Their Routes](#-6-message-types-and-their-routes)
- [🧹 Outbox Cleanup Mechanism (Garbage Collector)](#-7-outbox-cleanup-mechanism-garbage-collector)
- [🧭 Horizontal Scaling and Client Sharding](#-8-horizontal-scaling-and-client-sharding)
- [🛡️ Idempotency and Prevention of Message Resending](#-9-idempotency-and-prevention-of-message-resending)
- [🗄️ Outbox on PostgreSQL and Fetch Management](#-10-outbox-in-postgresql-and-fetching-management)
- [🧩 Project Folder Structure](#-11-project-folder-structure)
- [🧭 Summary and Future Development](#-summary-and-future-development)
- [🧩 PS. Advanced Scalable Acknowledgment and Cleanup Architecture (Tetris Model)](#-ps-advanced-scalable-acknowledgment-and-cleanup-architecture-tetris-model)
- [📖 PSS. Terms and Explanations](#-pss-terms-and-explanations)

---

**Reactive messaging module**, developed in **Java + Quarkus** using **Mutiny** and **Hexagonal Architecture** principles.  
The project is designed for building scalable, fault-tolerant, and reactive communication systems with guaranteed message delivery and adaptive load management.

---

## 🚀 Key Idea

The messenger is based on the concept of a **reactive pipeline**,  
where each message goes through a series of asynchronous stages: validation, persistence, delivery, acknowledgment, and finalization.

The system is built around a **clean domain** (core) and **configurable ports** (ports/adapters),  
making it flexible, testable, and easily extensible for specific infrastructures.

---

## 🧩 Core Technologies

| Component | Technology Used | Purpose |
|-----------|-----------------|---------|
| ☕ **Java 21+** | Main language | Performance, reactive model, type safety |
| ⚡ **Quarkus** | Application framework | Fast startup, low resource consumption |
| 🔁 **Mutiny** | Reactive library | Data streams, asynchronous pipelines |
| 🧠 **Hexagonal Architecture** | Architectural pattern | Isolation of business logic from infrastructure |
| 🧱 **PostgreSQL** | Persistent storage | Implementation of the Outbox pattern and message history |
| ⚙️ **Redis** | In-memory cache | Presence, idempotency, storing watermarks |
| 🔐 **Keycloak** | Authentication and authorization | Token validation, role-based access |
| 🌐 **API Gateway** | External gateway | Client load balancing and WebSocket session routing |

---

## 🧭 Project Ideology

**Reactive Messenger** is not just a chat.  
It is an architectural platform for building complex communication systems with guaranteed delivery and data flow control.

Key goals:
- 💬 **Guaranteed message delivery** with acknowledgments and fault tolerance;
- ⚙️ **Load management** (backpressure, buffers, credits, metrics);
- 🧩 **Flexibility and extensibility** via dynamic pipeline stage assembly;
- 🔒 **Security and access control** (OAuth2, Keycloak, roles);
- 🌍 **Scalability and fault-tolerance** through responsibility separation and horizontal sharding;
- 🧠 **Infrastructure independence** — all components can be replaced without changing the domain.

---

## ⚙️ 1. System Core — State Machine (Pipeline)

The core of the messenger is a **state machine**, implemented as a **flexible pipeline**.  
Each message goes through a set of stages determined by its type — chat, signaling, technical, etc.

### 🔩 Main Pipeline Stages
1. **Validation** — checking data integrity and access rights.
2. **Saving to DB** — persistence via a port (PostgreSQL or another storage).
3. **Sending ACK** — acknowledgment to the sender.
4. **Caching** — storing in Redis for fast access.
5. **Delivery** — transmitting to the receiver via WebSocket or another transport.
6. **Finalization** — closing the chain, error handling, and compensation.

The pipeline is **dynamically assembled by the resolver** based on configuration or business context, allowing for:
- easy addition of new message types;
- reuse of existing stages;
- adapting routes without changing the core code.

---

## ⚡️ 2. Reactive Model and Load Management

The system is built on **reactive streams**, where each stage works asynchronously and independently.

### 🧩 Buffering Mechanism
- Between stages, there are **reactive buffers** that equalize processing speed.
- In case of overflow, messages may be dropped with logging and metrics.
- All performance indicators are published to **Prometheus**.

### 🔁 Feedback Loop
If the system is overloaded:
- incoming data reading slows down (outbox = database);
- the system stabilizes without losing messages;
- once the load decreases, it automatically returns to normal operation.

---

## 💳 3. User Credit Mechanism

To prevent client overload, a **credit system** is used:
- each user is allocated a limited pool of credits for message sending;
- credits **replenish** over time (e.g., every 10 seconds);
- when the limit is reached, the message is **rejected**, and the client receives a notification.

This implements **backpressure at the client level**, preventing avalanche load.

---

## 🧱 4. Architectural Approach — Hexagonal Architecture

The architecture is divided into **core (domain)** and **infrastructure**:

| Layer | Description |
|-------|-------------|
| 🧠 **Domain** | Business logic of pipelines, stages, and resolvers |
| 🔌 **Ports and Adapters** | Integration with infrastructure (PostgreSQL, Redis, WebSocket...) |

This approach:
- isolates business logic from infrastructure;
- simplifies testing and technology replacement;
- ensures easy horizontal scaling.

---

## 🧾 5. Guarantees and Goals

- The **Outbox pattern** is used for guaranteed delivery.
- There is a **user presence cache** to avoid delivery attempts to inactive clients.
- Target performance: **≥ 1000 messages/sec** (with optimization potential).

---

## 💬 6. Message Types and Their Routes

### 💡 1. Regular Chat Message (Client → Client)
**Purpose:** transferring a user message with guaranteed delivery.

**Pipeline Stages:**
1. Receive from client
2. Validation (rights, format, credits)
3. Save to DB (Outbox)
4. ACK to sender
5. Attempt delivery to receiver
6. ACK from receiver
7. Update state in DB
8. Finalization (retry delivery if needed)

**Features:**
- Guaranteed delivery
- Retry attempts
- Deduplication during scaling

---

### 📞 2. Signaling Message (e.g., video call)
**Purpose:** transmitting real-time events **without persistence**.

**Stages:**
1. Receive from client
2. Minimal validation
3. Temporary storage (for deduplication)
4. ACK to sender
5. Send signal to receiver
6. Finalization

**Features:**
- Not saved in DB
- Short lifecycle
- Unique `messageId` + `senderId`

---

### 📎 3. Message with Attachment (files, images, videos)
**Purpose:** asynchronous content transfer with upload confirmation.

**Stages:**
1. Receive metadata
2. Validation
3. Generate `upload URL` and token
4. ACK to client
5. Upload file to storage (MinIO etc.)
6. Webhook from storage
7. Create technical message
8. Notify chat participants
9. Finalization

**Features:**
- Two-phase logic (`initiation` → `completion`)
- Asynchronous webhook cycle
- Integration with S3-compatible storage

---

### ⚙️ 4. Technical System Messages
**Purpose:** transferring control signals and notifications.

**Examples:**
- overload or credit limit warnings;
- connection disruption notifications;
- user state change events.

**Stages:**
1. Generate on server
2. Send directly to client
3. No ACK and no storage

---

### 🚀 5. Additional Types (for expansion)
- **Presence** events (user status)
- History synchronization
- Command messages (edit, react, delete)
- Monitoring and analytics events

---

## 🧹 7. Outbox Cleanup Mechanism (Garbage Collector)

To prevent the **Outbox** table from growing indefinitely and to maintain data relevance, the **watermark mechanism** is used, synchronized with the lifecycle of **WebSocket sessions**.

---

### 🔖 Core Idea

Each message is written **simultaneously** to two storage systems:
- **History** — long-term storage of all messages;
- **Outbox** — temporary buffer for delivering messages to active clients.

When a client **disconnects** from the WebSocket session (logout, timeout, disconnect), the server records a **watermark** — a mark of the last delivered message.  
This mark is stored in the **local cache** (or Redis, if distribution is required) and is used by the garbage collector for cleaning the Outbox.

---

### ⚙️ How It Works

1. **Creating Watermark**  
   When the connection is terminated, the `SessionManager` component records the identifier of the last delivered message or a timestamp (`timestamp`) in the `WatermarkRegistry`.

2. **Storing Watermark**  
   The watermark is saved in local memory or Redis and lives until the client reconnects.  
   It determines the lower boundary of messages that the client has already guaranteed to receive.

3. **Outbox Cleanup**  
   A periodic process (`GarbageCollector`), initiated by the `GarbageScheduler`, analyzes the watermarks of all clients.  
   For each client, all messages in the Outbox with a position **less than or equal to** the corresponding watermark are deleted.

4. **Safe Deletion**  
   Since all messages have already been saved in History, cleaning the Outbox **does not result in data loss**.  
   Upon reconnection, the client restores its context by requesting the history starting from the watermark.

---

### 🧩 Benefits

- Outbox stays compact and relevant.
- Cleanup does not break data consistency.
- Easily combines with any storage implementation (PostgreSQL, Redis, InMemory).
- Works independently of the number of instances and does not interfere with the main message processing thread.

---

> 💡 Outbox is a live delivery buffer,  
> History is a long-term storage,  
> Watermark + GarbageCollector is the mechanism that keeps the system clean and predictable.

---

## 🧭 8. Horizontal Scaling and Client Sharding

The messenger supports **horizontal scaling** through client distribution across instances at the **API Gateway** level.  
This separation eliminates races when working with a shared Outbox and makes message delivery deterministic.

---

### 🔩 Core Idea

- The **API Gateway** accepts incoming WebSocket connections and balances them **by ClientID**.
- The same `ClientID` is **always directed to the same messenger instance**.
- This is achieved using a **deterministic hash function** (e.g., `CRC32(ClientID) mod N`, where `N` is the number of instances).

Thus, each messenger instance receives:
- its own set of active clients;
- a local session registry;
- local watermarks;
- responsibility for delivery and cleanup of messages only for its own clients.

---

### ⚙️ Working with Shared Outbox

- All messenger instances use a **shared Outbox table** (PostgreSQL).
- When reading from the Outbox, each instance **selects only messages** belonging to its clients,  
  according to the same hash function rule that the API Gateway uses.
- This guarantees that a message will not be processed simultaneously by multiple instances.
- Outbox cleanup (via the GarbageCollector) is also performed **only for clients of the given instance**.

---

### 🧱 Advantages of This Approach

- 🔒 **No races:** Each instance works only with its “own” clients and their messages.
- ⚡ **Minimal locks:** The shared Outbox table does not require centralized leasing.
- 📈 **Linear scaling:** When adding new instances, the load is automatically redistributed.
- 🧩 **Compatibility:** The architecture does not require changes to the pipeline business logic.

---

### 🧠 Conclusion

> Scaling is built on the **"ClientID → Instance"** principle,  
> which makes message delivery deterministic, and the Outbox — safe and manageable.  
> All clients are served by their instance, and the system remains consistent regardless of the number of nodes.

---

## 🛡️ 9. Idempotency and Prevention of Message Resending

To protect against the resending of messages, an **idempotency** mechanism is implemented. This is necessary to ensure that the same message will not be sent again due to:

- Frequent triggering of the scheduler that reads the Outbox;
- Delayed ACK from the client;
- Network latency and internal processing delays;
- Parallel threads and races when retrying the send.

The mechanism safely ignores messages that have already been sent and are within their time-to-live (TTL).

---

### 🔹 TTL Calculation

The lifetime of an idempotency record in the buffer considers:

1. **Network Latency (`delta_network`)** — the time it takes for an ACK to arrive from the client.
2. **ACK Buffer (`delta_ack_flush`)** — delay before messages are batch-deleted from the Outbox.
3. **Scheduler Interval (`S`)** — period for reading new messages from the Outbox.
4. **Internal Latency (`delta_processing`)** — minimal delays in processing within the cluster.

**TTL Formula:**
```
TTL = delta_network + delta_ack_flush - S + delta_processing
```

**Example (for a typical load):**

- `delta_network` = 2 sec
- `delta_ack_flush` = 0.5 sec
- `S` = 1 s
- `delta_processing` = 0.1 sec

```
TTL = 2 + 0.5 - 1 + 0.1 = 1.6 sec ≈ 2 sec
```
So, the message will remain in the idempotency buffer for about **2 seconds** to guarantee protection against resending, taking into account network latency and scheduler cycles.

---

### 🔹 Algorithm

1. **When sending a message**
- The idempotency buffer is checked by `messageId`.
- If the record exists and `currentTime - timestamp < TTL`, the message **is not sent**.
- Otherwise, the message is sent, and the record is added to the buffer with the current timestamp.

2. **Removal from the buffer**
- Occurs **only after TTL expires** or when the buffer is full.
- ACK from the client does not affect the removal.
- Old records are removed based on FIFO principle to ensure a limited buffer size is always maintained.

3. **Buffer implementation**
- **Local memory**: The `ConcurrentInsertionOrderMap` structure provides O(1) access by `messageId` and FIFO clearing.
- **Redis**: Can be used for a distributed cluster, TTL is set using Redis' built-in features, but it introduces network latency.

---

### 🔹 Advantages

- Protection against resending in case of scheduler races and network delays.
- Minimization of load on WebSocket and internal services.
- High performance with O(1) access and FIFO clearing.
- Flexible implementation: locally in memory or via Redis for distributed systems.

---

## 🗄️ 10. Outbox in PostgreSQL and Fetching Management

Outbox implemented in a relational database ensures reliable management of guaranteed message delivery, especially when simultaneously inserting a record in the Outbox and the persistent message history store.

- **Insertion**: Can be performed **in a single transaction**, to simultaneously update the Outbox and message history.
- **Fetching messages for sending**: It is recommended to use **`SELECT ... FOR UPDATE`** with **leasing**.
- Leasing is a temporary marker that prevents the same message from being re-fetched by other instances of the scheduler.
- The leasing time should be a multiple of the scheduler cycle to ensure that each record is sent at least once before becoming available for the next fetch.
- **Scheduler periodicity**: Selected based on load and batch size. For example, if the scheduler fetches every 1 second, leasing can be set to 3 seconds (three scheduler cycles).

**Advantages of relational Outbox:**

- Simple guarantee of atomicity with message history.
- Support for multiple instances without race conditions.
- The ability to finely tune fetch periodicity and leasing for optimal performance.

---

> ✅ In summary: Idempotency and Outbox in PostgreSQL ensure guaranteed message delivery, protection against resends, and load control at high operation frequencies.

---

## 🧩 11. Project Folder Structure

The project is organized based on Hexagonal Architecture (Ports & Adapters), ensuring modularity, testability, and independence of business logic from infrastructure.

🧠 config/

Configuration classes of the application:

- **KeycloakConfig** — Integration setup with Keycloak for authentication and authorization.
- **MessengerConfig** — Messenger configuration: batch parameters, timeouts, and other system options.

⚙️ core/

Main business logic of the application. All key processes for routing, regulation, and state management are concentrated here.

- **backpressure/** — Data flow management and load limiting through reactive publishers and metrics.
- **credits/** — Message credit management (limits by channels, filtering, lazy computations).
- **feedback/** — Adaptive feedback, collection of health events, and dynamic adjustment of polling frequency.
- **garbage/** — Collection and cleanup of outdated data, including asynchronous outbox collectors.
- **poller/** — Periodic polling of the outbox and message publication (pollers).
- **presence/** — Client presence state management (coordinators, synchronization).
- **router/** — Routing of incoming and outgoing messages through pipeline stages. Includes logging and handling ack/outbox/cache stages.
- **session/** — Local session registry and cleanup strategies.
- **watermark/** — Generation and storage of watermarks for tracking processing progress.

🏛️ domain/

Domain entities and interfaces — independent from frameworks and infrastructure. They define what the system does, not how.

- **generator/** — Message ID generators (e.g., ULID).
- **message/** — Basic message models: Message, MessageEnvelope, MessageType.
- **session/** — Domain model of a client session (ClientSession, ClientData).
- **stage/** — Statuses of message processing stages.
- **validator/** — Interfaces and implementations for validating incoming data.
- **watermark/** — Domain model of watermarks (position, state marker).

🧰 infrastructure/

Port implementations (adapters) ensuring connection between the core layer and external systems: DB, cache, WebSocket, and REST.

- **cache/** — Cache implementations (in-memory, Redis): idempotency, presence, and watermark.
- **persistence/** — Storage implementation (in-memory and Postgres) for history and outbox.
- **rest/** — REST API for accessing message history and presence state, including token validation.
- **websocket/** — Management of WebSocket connections: reception, delivery, transformation, security, and notifications.

🔌 ports/

Contracts for interaction between the core and infrastructure.  
Interfaces are defined here through which the core interacts with the external world.

- **channel/** — Message delivery channel interface.
- **history/** — Message history access interface.
- **idempotency/** — Idempotency manager interface.
- **notifier/** — Notification system interface.
- **outbox/** — Outbox table interface.
- **presence/** — Client presence manager interface.
- **session/** — Session management interface.
- **watermark/** — Watermark registry interface.

⏰ scheduler/

Task schedulers responsible for periodically executing actions.

- **RoutingScheduler** — Polling the outbox and distributing messages.
- **GarbageScheduler** — Periodic cleanup of outdated data and caches.
- **TestScheduler** — Utility scheduler for debugging and testing.

## 🧭 Summary and Future Development

The **reactive messenger** project is actively being developed and is gradually moving from the architectural framework to full implementation of all message exchange components.

### 🎯 Immediate Goals
- **Domain expansion (DOMAIN):** Adding routing business logic, acknowledgments, error handling, and compensation.
- **Improving load resilience:** Adaptive flow control, task prioritization, latency control, and stage balancing.
- **Handling inactive clients:** Introducing session monitoring, automatic termination of inactive connections, and notification dispatch.
- **Scaling development:** Improving client distribution across instances, stable horizontal sharding via API Gateway.
- **Expanding metrics and observability:** Integration of new metrics for latency, errors, buffers, and reactive stages (export to Prometheus).
- **Multilevel testing:** Checking performance and stability in various environments (local, cluster, Docker Compose).
- **Infrastructure integrations:** Completing port implementations for PostgreSQL (Outbox, history) and Redis (presence cache, watermark, idempotency).

### 🌱 Strategic Direction
The project will continue to evolve as a **universal reactive messaging platform**,
which can be used as a foundation for:
- Enterprise messengers;
- Real-time notification systems;
- Client state synchronization services.

The goal is to create an **open, reliable, and observable reactive messaging module**,
capable of operating stably under high load, scaling horizontally, and easily integrating into microservice architectures.

---

## 🧩 PS. Advanced Scalable Acknowledgment and Cleanup Architecture (Tetris Model)

This option is an advanced architecture for buffering, idempotency, and watermark control,  
based on the analogy with the **“Tetris”** game 🎮.  
Each ACK event fills a “cell” in the offset stream, and the ACK aggregator forms “layers” that can be safely deleted once they are completely filled.

---

### ⚙️ Message Processing Flow

1. **PostgreSQL** — record the message in history (and, if needed, in the outbox).
2. **Debezium → Kafka** — transactional changes are streamed to the `topic messages`, partitioned by `clientId` or `clientShard`.
3. **Messenger Consumer** reads from Kafka and delivers the message to the WebSocket client.
4. **The client sends an ACK**, which is saved in Redis or published to Kafka (`topic acks`).
5. **ACK Aggregator** (Kafka Streams / separate service) collects ACK events and *merges intervals* (like “Tetris layers”), forming compact ranges per-client.
6. Based on the aggregated ranges and watermarks, the **`global_safe_offset`** is calculated — the minimum right boundary across all clients.
7. This `global_safe_offset` serves as the logical boundary for **cleaning up the outbox / Kafka / Postgres**.

---

### 🔁 Replay and Delivery

The Messenger-consumer periodically performs a “replay”:
- It returns to the `logicalBasePosition` (the minimum point of the unread range);
- It re-reads Kafka and filters out already acknowledged messages based on the aggregated ranges;
- It re-delivers only undelivered messages.

This guarantees **idempotent delivery** and resilience in case of failures, overloads, or temporary network issues.

---

### 🧱 State Storage Options

#### 🅰️ Option A — Redis (runtime) + Postgres (backup)
**Recommended approach.**

Redis:
- Fast in-memory store for ACK ranges, watermarks, and `global_safe_offset`;
- ZSET to store `right_bound` of each client;
- Lua scripts for atomic interval merge.

Postgres:
- Long-term snapshot storage (periodic persist for recovery after failure).

**Typical Redis Keys:**

```
client:{id}:ranges → JSON/CBOR list of intervals [[s1,e1],[s2,e2],...]
clients:right_bounds → ZSET (score = right_bound, member = clientId)
client:{id}:watermark_ts → timestamp of the last activity
global:safe_offset → current global minimum
idempotency:{msgId} → TTL entry for duplicate protection
```


**Key Operations:**
- On ACK:
   1. Read current intervals `client:{id}:ranges`
   2. Merge with new offset (merge)
   3. Update `right_bound` and ZSET
   4. Recalculate `global:safe_offset` if necessary
   5. Asynchronously persist snapshot in Postgres

**Advantages:**
- Instant filtering during send (`O(1)` on ranges)
- Fast aggregation of global minimum (`ZRANGE 0 0`)
- Minimal delivery delay (µs–ms)

**Disadvantages:**
- Redis requires clustering for a large number of clients
- A mechanism for periodic snapshots is required for state recovery

---

#### 🅱️ Option B — Kafka Streams / ksqlDB + Compacted Topics
**Alternative option without Redis.**

- `topic acks` → Kafka Streams App → aggregates ACK by `clientId`
- Result is stored in compacted topic `ack_ranges`
- A separate stream calculates `global_safe_offset` and saves it in the compacted topic `global_offset`

**Advantages:**
- High fault tolerance and horizontal scalability;
- Built-in durable storage without an external DB;
- Automatic replication of the state store.

**Disadvantages:**
- Requires Kafka Streams infrastructure;
- Slightly higher latency (but not critical for ACK aggregation).

---

### 🧮 Merge Algorithm (Tetris Merging)

Each ACK is an offset `o`.

We store intervals `[ [s1,e1], [s2,e2], ... ]` (non-overlapping and sorted).

When inserting a new `o`:
1. Find the intervals that are adjacent on the left/right.
2. Merge them (possibly multiple intervals).
3. Update the structure and the right boundary.

**Implementation Options:**
- In a Java service (Quarkus) — simple and fast.
- In Redis — using Lua script for atomicity (merge + HSET in a single transaction).

---

### 🌍 Calculating `global_safe_offset`

```text
1. ZSET clients:right_bounds — stores the right_bound of all clients.
2. global_safe_offset = ZRANGE clients:right_bounds 0 0 WITHSCORES.
3. When updating the right_bound of a client → recalculate global_safe_offset.
4. For inactive clients, use leaveStamp → remove from ZSET.
```

## 📖 PSS. Terms and Explanations

| Term                               | Definition / Explanation                                                                                   |
|------------------------------------|------------------------------------------------------------------------------------------------------------|
| **Reactor / Reactive model**       | An event-driven processing model where components react to data asynchronously and non-blocking.            |
| **Pipeline (conveyor)**            | A sequence of stages for message processing: validation → write → delivery → ACK.                           |
| **Mutiny**                         | A reactive library for Quarkus that provides declarative asynchronous data streams.                         |
| **Hexagonal Architecture (Ports and Adapters)** | An architecture that separates business logic (domain) and infrastructure for independence and testability. |
| **Outbox**                         | A buffer where messages are saved before sending — guarantees delivery.                                     |
| **History**                        | The main long-term storage for messages (as opposed to temporary Outbox).                                   |
| **Watermark**                      | A timestamp marking the disconnection (session) of a client. Used during Outbox cleanup in conjunction with ACK. |
| **Garbage Collector (GC)**         | A process that removes outdated records from the Outbox based on the Watermark.                             |
| **ACK (Acknowledgment)**           | Confirmation from the client of the message delivery.                                                       |
| **Backpressure**                   | A mechanism for limiting the incoming data flow to avoid overloading the system.                            |
| **Credits system**                 | A counter for message limits per user to implement client-side backpressure.                               |
| **Idempotency**                    | A guarantee that re-sending the same message will not alter the result.                                     |
| **Leasing**                        | A mechanism for temporarily locking a record when reading from the Outbox to avoid duplicates.              |
| **Presence**                       | The "online/offline" state of the user, cached in Redis.                                                    |
| **Session Manager**                | A component that manages the lifecycle of client WebSocket sessions.                                        |
| **Redis**                          | A fast in-memory DB for caching state, watermarks, idempotency, and presence.                               |
| **Prometheus**                     | A monitoring system where the messenger exports performance metrics.                                        |
| **Quarkus**                        | A Java framework for reactive and native applications with fast startup and low resource consumption.        |
| **Tetris Model**                   | An architectural metaphor for combining ACK ranges, where filled “layers” can be safely cleaned up.         |
| **CRC32(ClientID) mod N**          | A method for deterministic client distribution across instances when scaling.                               |

---

> 💡 *This section can be expanded as new components and terms are added to the system.*

> ⚙️ _We are building the foundation of a reactive communication platform,  
> where performance, scalability, and architectural purity are not a compromise, but the standard._
