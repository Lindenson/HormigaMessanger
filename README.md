# 📨 Hormigas Messenger

> Real-time **master ↔ client chat** for the Hormigas services marketplace — a reactive,
> guaranteed-delivery WebSocket service built on **Java 25 + Quarkus + Mutiny** with a clean
> **Hexagonal** core.
>
> 🌍 Other languages: [Russian](./README.ru.md)

It sits alongside the other Hormigas services (MasterProfile `:8080`, ClientProfile `:8081`,
Order/TaskManager `:8082`) and lets the two parties of a job — a **master** and a **client** —
coordinate: discuss scope, agree terms, share files, and receive system notifications.

> **Source-of-truth note.** This README is generated from the approved **concept** and the
> **functional requirements (FR)** held in the architecture digital twin
> (`knowledge/concepts/messenger-{concept,use-cases,functional-requirements}.md`). The twin docs
> remain authoritative; this README is the public-facing view.

---

## 📘 Contents

- [What it is](#-what-it-is)
- [Conversation model](#-conversation-model)
- [Message kinds & handling strategies](#-message-kinds--handling-strategies)
- [Message states & lifecycle](#-message-states--lifecycle)
- [Mutability, freeze & retention](#-mutability-freeze--retention)
- [Core guarantees](#-core-guarantees)
- [Architecture — Hexagonal core + reactive pipeline](#-architecture--hexagonal-core--reactive-pipeline)
- [Delivery engine — Outbox + History + Watermark/Tetris GC](#-delivery-engine--outbox--history--watermarktetris-gc)
- [Presence ↔ GC coupling](#-presence--gc-coupling)
- [Idempotency](#-idempotency)
- [Reconnect & history sync](#-reconnect--history-sync)
- [Authentication & authorization](#-authentication--authorization)
- [HTTP & WebSocket API](#-http--websocket-api)
- [Configuration](#-configuration)
- [Persistence schema](#-persistence-schema)
- [Build, run & test](#-build-run--test)
- [Status vs functional requirements](#-status-vs-functional-requirements)
- [Roadmap & deferred work](#-roadmap--deferred-work)
- [Project structure](#-project-structure)
- [Appendix A — Tetris ACK/GC model](#-appendix-a--tetris-ackgc-model)
- [Appendix B — Glossary](#-appendix-b--glossary)

---

## 🚀 What it is

A **reactive pipeline** messenger: every message flows through asynchronous stages
(validate → persist → ACK → deliver → finalize), assembled dynamically by a resolver from the
message type. A **clean domain (core)** is isolated from infrastructure by **ports/adapters**, so
PostgreSQL, Redis and WebSocket transports can be swapped without touching business logic.

### Core technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| ☕ **Java 25** | Language | Records, pattern matching, performance |
| ⚡ **Quarkus 3.32** | Framework | Fast startup, low footprint, `websockets-next` |
| 🔁 **Mutiny** | Reactive library | Non-blocking `Uni` pipelines |
| 🧠 **Hexagonal architecture** | Pattern | Domain isolated from infrastructure |
| 🧱 **PostgreSQL** (reactive PG client + Flyway) | Durable store | Outbox + message History + conversations |
| ⚙️ **Redis** | In-memory state | Presence, idempotency, Tetris watermarks |
| 🔐 **Ory** (Kratos + Oathkeeper) | Identity | Proxy-injected identity headers (no JWT in-app) |
| 🌐 **API Gateway** | Edge | TLS, identity injection, WS routing, future sharding |

---

## 🧭 Conversation model

- **Identity** — a conversation is the **pair `(clientId, masterId)`** (two Ory identities).
  **1:1 only; no group chats.**
- **Universal, idempotent creation** — one core *create-chat* operation, invoked by interchangeable
  inbound adapters. Same logical pair → the existing chat is returned, never duplicated.
  - ✅ **REST** adapter (`POST /api/chats`) and **admin/service** calls — implemented.
  - 🟡 **Order event** adapter ("a master expressed interest in an order", Kafka `order.events`) —
    **deferred** (see [Roadmap](#-roadmap--deferred-work)). The Kafka listener is just another
    adapter over the same core op.
- **Order-agnostic** — **one conversation per pair, reused across orders.** The messenger never
  models/branches/groups by order; the `orderId` travels only as **opaque metadata**. Grouping by
  order is a **frontend** concern.
- **Soft delete (per participant)** — either side can hide the chat from their own view; it is
  **never removed from the system** and the peer is unaffected.
- **Blacklist/block (per participant)** — either side can block the other; while blocked, sending
  between them is rejected. Unblock restores messaging.

> **Dual-driven principle.** Every event-capable operation (create chat, freeze) is **one core use
> case** exposed via **two inbound adapters — REST and an event consumer**. Triggers are
> interchangeable; the use case is the mechanism. (Today: REST is live, the event consumer is deferred.)

---

## 💬 Message kinds & handling strategies

Each message kind maps to one of four **handling strategies** (orthogonal properties, resolved per
`MessageType`):

| Strategy | Durably persisted | Retried until ACK | Idempotent | Kept in History | Used for |
|----------|:---:|:---:|:---:|:---:|----------|
| **A — persistent** | yes | yes | yes | yes (until TTL; frozen → own longer TTL) | chat messages |
| **B — fire-and-forget** | no | no | no | no | typing, presence |
| **C — retry-then-purge** | transient (outbox) | yes | yes | no | must-arrive system notices |
| **S — signaling** | no (transient) | yes | yes | no | **WebRTC** call setup |

### Wire `MessageType` values

```
CHAT_IN     CHAT_OUT     CHAT_ACK        # persistent chat (Strategy A)
SIGNAL_IN   SIGNAL_OUT   SIGNAL_ACK      # WebRTC signaling (Strategy S)
PRESENT_INIT  PRESENT_JOIN  PRESENT_LEAVE   # presence (Strategy B)
SERVICE_OUT                              # server→client technical message
```

The server assigns the canonical `messageId` (a **ULID** — time-monotonic) on inbound; the client's
own id travels as `correlationId` for ACK matching and frontend dedup.

---

## 🔄 Message states & lifecycle

Persisted status machine: **`SENT → DELIVERED → READ`**.

- **SENT** — message durably written (History + Outbox in one transaction) and ACKed to the sender.
- **DELIVERED** — set the moment the message is actually pushed to the recipient over WS.
- **READ** — persisted when the recipient acknowledges via `POST /api/chats/{id}/read`.

### Persistent (Strategy A) happy path

1. Client → WS `CHAT_IN` `{conversationId, messageId, payload, metadata{orderId}}`.
2. **Validate** — membership (sender in the pair, not blacklisted), format, timezone.
3. **Persist atomically** — History + Outbox in a single transaction.
4. **ACK sender** (`CHAT_ACK`, status `SENT`).
5. **Deliver** — if the recipient is online, push `CHAT_OUT` (→ `DELIVERED`); else hold in Outbox.
6. **Read receipt** → status `READ`.
7. **GC** — remove Outbox rows ≤ the safe-delete watermark; History is retained per TTL.

---

## 🧊 Mutability, freeze & retention

- **Immutable** — messages are write-once; **there is no edit operation.**
- **Conditional delete** — a participant may delete a message **only while it is NOT frozen**;
  a frozen message → `409`.
- **Freeze is message-level, scoped by `orderId`** — there is **no chat-level freeze**. When the
  parties reach a contract for an order, *that order's* messages are frozen (one-way) and become
  immutable evidence. A sibling order's messages in the **same** chat stay deletable.
  `POST /api/chats/{id}/freeze {"orderId": "..."}`.
- **Retention classes** — normal history uses the per-kind TTL; **frozen** messages have their
  **own, longer** TTL (a separate class, not exemption). The repository honours both
  (`deleteOlderThan` excludes frozen; `deleteFrozenOlderThan` for the frozen class). *(Scheduled
  wiring of the retention sweeps is a remaining task.)*

---

## 🧾 Core guarantees

1. **Persistent chat — never lost.** Stored durably before any ACK.
2. **Offline delivery.** Held in Outbox and delivered on the recipient's next connection.
3. **At-least-once + best-effort idempotency.** The server strives not to resend; exactly-once is
   **not** guaranteed — the **frontend dedups by `messageId`**.
4. **Read receipts** persisted (`SENT → DELIVERED → READ`).
5. **Per-conversation ordering** — *targeted; not yet implemented* (see Roadmap).

---

## 🧱 Architecture — Hexagonal core + reactive pipeline

```
domain/   — framework-free models (Message, Conversation, ClientData, …) and contracts
core/     — business logic: pipeline stages, resolver, pollers, watermark, presence, GC
ports/    — driving/driven interfaces (history, outbox, channel, presence, tetris, message, …)
infrastructure/ — adapters: Postgres, Redis, WebSocket, REST
```

The **pipeline** is a state machine assembled per message type by a resolver
(`EnumMap<MessageType, PipelineType>`):

1. **Validation** — integrity, membership, access.
2. **Persist** — History + Outbox (Strategy A), via a port.
3. **ACK** — to the sender.
4. **Cache** — Redis for idempotency/fast access (cached strategies).
5. **Delivery** — over WebSocket; marks `DELIVERED` on a successful push.
6. **Finalization** — error handling / compensation.

A **HexagonalArchitectureTest** (ArchUnit) ratchets layering at build time: the domain stays
framework-free, the core depends only on ports, and ports never depend on core or infrastructure.

### Reactive model & load management

Stages run asynchronously with reactive buffers between them; on overload, outbox reading slows
(adaptive `Regulator` feedback) and recovers automatically. All throughput/latency/buffer metrics
export to **Prometheus** (`/q/metrics`).

---

## 🗄️ Delivery engine — Outbox + History + Watermark/Tetris GC

- **Own PostgreSQL outbox.** Incoming persistent messages land in `outbox`; a poller reads directly
  from Postgres (**no Kafka in the internal engine**), delivers, and rows are cleaned once confirmed.
- **History** is the durable source of truth; **Outbox** is the live delivery buffer.
- **Leasing.** The poller claims a batch in **one statement** —
  `UPDATE outbox … FROM (SELECT id … WHERE lease_until <= now() ORDER BY id LIMIT n FOR UPDATE SKIP LOCKED) … RETURNING …` —
  so multiple instances never process the same row, with a single round-trip per poll.
- **"Tetris" watermark GC.** Per-recipient ACK ranges are merged in **Redis** (ZSETs + atomic **Lua**
  scripts) to compute the **global safe-delete id**; the GC deletes Outbox rows ≤ that id. History is
  retained per TTL. Scripts run via `EVALSHA` with a **`NOSCRIPT` fallback** (survives a Redis
  restart); startup cleanup uses `SCAN` + `UNLINK` (never the blocking `KEYS`). See
  [Appendix A](#-appendix-a--tetris-ackgc-model).

---

## 🟢 Presence ↔ GC coupling

Presence is a **correctness dependency of GC**: the watermark advances on delivery, which requires
the recipient to be *accurately* online — a phantom-online client stalls GC. Therefore:

- **Presence (Redis)** tracks who is connected and gates live delivery (offline → hold in Outbox).
  Online/offline transitions are ephemeral (Strategy B) events to the peer.
- **Mark OFFLINE on delivery failure** — if a live push to an open session fails, the recipient is
  transitioned OFFLINE so the poller stops phantom-online retries and simply holds in Outbox.
- **Heartbeat / ping** — WebSocket auto-ping detects dead connections; stale sessions are cleaned up.

---

## 🛡️ Idempotency

- **Server:** best-effort dedup via a short-TTL buffer keyed by `messageId` (Redis), sized to cover
  network + ACK-flush + scheduler latency.
- **Client:** authoritative dedup by `messageId` — which is why exactly-once is not a server goal.
  Rare wire duplicates are acceptable; the user never sees one.

---

## 🔌 Reconnect & history sync

On (re)connect a client **pulls its conversation history via REST**, then resumes live WS delivery —
this read-through is what guarantees nothing is missed across disconnects.

History sync is **conversation-scoped and cursor-paginated**:
`GET /api/chats/{id}/messages?since=<last messageId>&limit=<n>` (default 200, max 500). Because
`messageId` is a ULID, it doubles as the page cursor (`WHERE message_id > $since ORDER BY message_id`).

---

## 🔐 Authentication & authorization

- **Ory** identity, injected by the edge (Oathkeeper) as headers — the service trusts them, it does
  **not** validate JWTs itself:

  | Header | Meaning |
  |--------|---------|
  | `X-User-Id` | caller identity (required) |
  | `X-User` | display name |
  | `X-Role` | `MASTER` / `CLIENT` |
  | `X-User-Email` | email |

  Missing/blank identity → WS close / HTTP `401`.
- **Authorization by conversation membership** — only the two participants may read/write a chat;
  others get `403`. Blacklisted pairs cannot message.

---

## 🌐 HTTP & WebSocket API

### WebSocket

```
ws://<host>/ws        # identity via the headers above; send/receive Message JSON frames
```

### REST — `/api/chats`

| Method & path | Purpose | Notable responses |
|---------------|---------|-------------------|
| `POST /api/chats` | Create/return chat for `{clientId, masterId, metadata}` (idempotent) | `201` created / `200` existing |
| `GET /api/chats` | List the caller's chats (excl. soft-deleted, recent-first) | `200` |
| `GET /api/chats/{id}/messages?since=&limit=` | Cursor-paginated history (membership-gated) | `200` / `403` / `404` |
| `DELETE /api/chats/{id}` | Soft-delete (per participant) | `204` |
| `POST /api/chats/{id}/block` · `DELETE …/block` | Blacklist / unblock the peer | `204` |
| `DELETE /api/chats/{id}/messages/{messageId}` | Delete a message (only if not frozen) | `204` / `404` / `409` frozen |
| `POST /api/chats/{id}/freeze` `{orderId}` | Freeze that order's messages (one-way) | `200 {frozen:n}` / `400` no orderId |
| `POST /api/chats/{id}/read` | Recipient marks their received messages READ | `200 {read:n}` |
| `GET /api/chats/{id}/receipts` | Per-message status (`SENT`/`DELIVERED`/`READ`) | `200` |

### REST — auxiliary

| Method & path | Purpose |
|---------------|---------|
| `GET /api/history` | Caller-scoped message history |
| `GET /api/presence` | Presence snapshot |
| `GET /q/health`, `GET /q/metrics` | Health & Prometheus metrics |

---

## ⚙️ Configuration

Runs under the **`prod`** profile; all hosts/secrets are environment-parameterised (never hardcoded).

| Env var | Default | Purpose |
|---------|---------|---------|
| `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` | `postgres` / `5432` / `hormigasdb` / `ant` / `ant` | PostgreSQL |
| `REDIS_HOST` / `REDIS_PORT` | `redis` / `6379` | Redis |

Tunables live under `processing.*` in `application.yaml` (outbound batch size, poll interval,
adaptive feedback factors, channel retry/backoff, credits, Tetris collector, idempotency TTL).
WebSocket max message size is 64 KiB.

---

## 🧮 Persistence schema

Flyway migrations (`src/main/resources/db/migration`):

| Migration | Adds |
|-----------|------|
| `V2` | `message_history` + `outbox` (atomic insert) |
| `V3` | `conversation` (pair, JSONB metadata) |
| `V4` | per-participant hide / block flags |
| `V5` | `message_history.frozen` |
| `V6` | `message_history.status` (`SENT`/`DELIVERED`/`READ`) |
| `V7` | `message_history.order_id` (+ index) — queryable order key for order-scoped freeze |

---

## 🛠️ Build, run & test

**Build & unit/integration tests** (JDK 25; Dev Services spins up PostgreSQL + Redis in Docker):

```bash
JAVA_HOME=/path/to/jdk-25 ./mvnw test
```

**Run locally** (against local infra, prod profile):

```bash
docker compose -f e2etests/docker-compose.yml up -d        # Postgres + Redis (+ MinIO)
./mvnw -DskipTests package
DB_HOST=localhost REDIS_HOST=localhost \
  java -Dquarkus.profile=prod -jar target/quarkus-app/quarkus-run.jar   # → :8080
```

**End-to-end acceptance** (Karate; app + infra must be up). The suite is ATDD — one feature per UC
group; `@wip` scenarios are excluded from the default run:

```bash
cd e2etests && JAVA_HOME=/path/to/jdk-21 mvn test -Dkarate.env=dev
```

Current coverage: **154 unit/integration tests + 17 Karate scenarios**, all green.

---

## ✅ Status vs functional requirements

**Implemented & tested:** chat lifecycle (idempotent create, list, soft-delete, blacklist),
send-guard + `SENT` ACK, online delivery + offline hold, `SENT→DELIVERED→READ` + read receipts,
immutability + conditional delete + **order-scoped freeze**, frozen retention class (primitive),
presence + **offline-on-delivery-failure**, watermark GC, **cursor-paginated** reconnect history,
Ory auth + membership authorization, opaque metadata + tz timestamps.

**Partial / deferred:**

- 🟡 **Per-conversation ordering** (FR-MSG-07) — not yet implemented.
- 🟡 **Event (Kafka) adapters** for create-chat & freeze (FR-CHAT-01 / FR-ARCH-01 / FR-RET-03) —
  deferred; REST side is live.
- 🟡 **Read-receipt push to sender** (FR-MSG-06) — status is persisted & polled; no push yet.
- 🟡 **Retention scheduler** (FR-RET-04/05) — primitives exist, not yet scheduled.
- 🟡 **WebRTC signaling** (FR-SIG) — pipeline routes exist, no e2e test.
- ⚪ **Credit rate-limiting** (FR-SEC-03) — optional v1, deferred.
- ⚪ **Attachments** (MinIO two-phase upload) — designed, not built.

---

## 🗺️ Roadmap & deferred work

- **Per-conversation ordering** — keyed delivery lane + single delivery authority.
- **Order integration** — Kafka consumer of "master interested in order" → create chat; contract
  event → freeze (completes the dual-driven model).
- **Retention scheduler** — wire `deleteOlderThan` / `deleteFrozenOlderThan` to a scheduled sweep
  with configurable TTLs.
- **Horizontal sharding** — `CRC32(clientId) mod N` at the gateway + per-instance Outbox filtering
  (designed for; Redis already centralizes shared state).
- **Attachments**, **read-receipt push**, **signaling e2e tests**.

---

## 🧩 Project structure

```
domain/         message/ (Message, MessageType, Payload), conversation/ (Conversation),
                credentials/ (ClientData), generator/ (IdGenerator), stage/, validator/, watermark/
core/           router/ (pipeline, stages, resolver, context), poller/ (outbox),
                garbage/ (collector), watermark/ (tetris), presence/, credits/, feedback/,
                backpressure/, conversation/ (ConversationService)
ports/          history, outbox, channel, presence, tetris, message (ReadReceipts, MessageModeration),
                idempotency, notifier, session
infrastructure/ persistance/postgres (+ inmemory), cache/redis, websocket, rest, security, generator
```

---

## 🧩 Appendix A — Tetris ACK/GC model

The safe-delete boundary is computed by merging per-recipient ACK ranges — the **"Tetris"**
metaphor: each ACK fills a cell, full layers can be cleared.

**Implemented variant — Redis + Lua (v1).** Redis structures:

```
tetris:re:<id>:ack   ZSET   — a recipient's unacknowledged message ids
tetris:minids        ZSET   — smallest pending id per client (score = id)
tetris:re:cnt        HASH   — pending count per client (heavy-client detection)
tetris:lastid        STRING — last globally seen id (forward progress)
```

Atomic Lua scripts handle `onSent` / `onAck` / `onDisconnect` and `computeGlobalSafeDeleteId`
(smallest non-zero `tetris:minids` score, else `lastid + 1`). Scripts are invoked by `EVALSHA`
with a `NOSCRIPT`→`EVAL` fallback.

**Deferred alternative — Debezium / Kafka-Streams.** A heavier variant (Postgres → Debezium →
Kafka, ACK aggregation in Kafka Streams, compacted `ack_ranges`/`global_offset` topics) is
documented as a possible future scaling path but is **not used** — the internal delivery engine is
PG-outbox + Redis only.

---

## 📖 Appendix B — Glossary

| Term | Meaning |
|------|---------|
| **Conversation** | The `(clientId, masterId)` pair; the unit of chat. 1:1, order-agnostic. |
| **Pipeline** | Per-type sequence of async stages: validate → persist → ACK → deliver → finalize. |
| **Outbox** | Live delivery buffer in Postgres; rows cleared once confirmed. |
| **History** | Durable long-term message store (source of truth). |
| **Watermark** | The safe-delete boundary; Outbox rows ≤ it are GC'd. |
| **Tetris** | ACK-range merge (Redis ZSET + Lua) computing the global safe-delete id. |
| **Leasing** | `FOR UPDATE SKIP LOCKED` + lease so instances don't double-process Outbox rows. |
| **Presence** | Redis-cached online/offline state; gates delivery and GC correctness. |
| **Freeze** | One-way, message-level, order-scoped immutability (contract evidence). |
| **ULID** | Time-monotonic message id; also the history page cursor. |
| **Strategy A/B/C/S** | persistent / fire-and-forget / retry-then-purge / signaling. |
| **Idempotency** | Best-effort server dedup by `messageId`; client is authoritative. |
| **Backpressure** | Adaptive slow-down of Outbox reading under load (Regulator feedback). |
| **Ory (Kratos/Oathkeeper)** | Identity; the edge injects `X-User-*` headers the service trusts. |

---

> ⚙️ _A reactive communication core where guaranteed delivery, clean layering, and observability
> are the standard — now shaped to the Hormigas master↔client domain._
