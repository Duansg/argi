# Redis State Foundation

Status: First implementation batch, authorized by the user on 2026-10-06.

## Objective

Provide an actual Redis-backed shared Store and an explicit atomic versioned
Store contract, while preventing Redis checkpoint read contention from being
mistaken for missing state. This batch introduces no MQ and does not claim to
implement distributed Worker failover or checkpoint-executor CAS.

## Compatibility And Ownership

- Java 17, existing Maven coordinates, packages, and `argi.*` defaults remain.
- Existing `Store`, `BaseCheckpointSaver`, graph APIs, checkpoint formats, and
  legacy no-argument Redis-like Store behavior remain available.
- Core owns additive `VersionedStore` and `VersionedStoreItem` contracts.
- Extensions owns the real Redis implementation in the existing
  `argi-graph-persistence-redis` module, using its existing Redisson dependency.
- RedisSaver lock timeout becomes an explicit read failure in both repositories;
  genuinely missing checkpoints remain empty. Interrupted reads restore the
  interrupt flag before throwing. Legacy Redis keys and serialization are unchanged.
- Work stays on local branches. No push, PR, merge into main, dependency changes,
  or user-file cleanup is part of this batch.

## Versioned Store Contract

`VersionedStore extends Store` adds three operations:

```java
VersionedStoreItem getVersionedItem(List<String> namespace, String key);
boolean putItemIfVersion(StoreItem item, long expectedVersion);
boolean deleteItemIfVersion(List<String> namespace, String key, long expectedVersion);
```

`VersionedStoreItem` is an immutable envelope containing
`Optional<StoreItem> item` and nonnegative `long version`. A never-written key
has no item and version zero. Live items have positive versions. A deleted item
keeps a positive tombstone version so a stale caller cannot recreate it with an
old expected version. The contained StoreItem follows the existing mutable DTO
contract; the envelope does not claim deep immutability.

Conditional writes/deletes return false on version mismatch without changing
data or version. Deleting an absent item returns false. Every successful mutation
increments its version once. Counter exhaustion fails before mutation; it never
wraps. Legacy Store implementations are not given a silent no-op CAS fallback.

## Redis Implementation

The new `io.github.agentic.ai.graph.persistence.redis.RedisStore` uses an
explicit Redisson client and configurable storage key. All records for one
Store live in one Redis Hash. Field identities reuse BaseStore's structured
namespace/key encoding. Each field contains one JSON envelope with both the
version and StoreItem, so reads cannot mix payload and version from separate
commands.

Mutations compare and replace the complete serialized old envelope atomically;
creation compares absence. Use Redisson's atomic conditional map operations
after verifying their implementation, or a single-key Lua compare-and-replace
script. Version arithmetic stays in Java to preserve the full long range.
Unconditional put/delete use bounded read-CAS retries, not local-only locks.

Deletes and clear retain tombstones, rather than resetting counters. This batch
sets no TTL on authoritative records. Search, namespace listing, size, and clear
scan records and exclude tombstones; they are not cross-item snapshot transactions.
Clear requires quiescent writers for a complete purge. This is a correctness-first
Store, not an indexed vector-search service. Persistent tombstone reclamation
requires a later generation/retention design.

Existing BaseStore validation, matching, sorting, and pagination conventions are
reused. Namespace listing excludes ancestors outside a requested prefix.
Separate storage keys are isolated, and client ownership stays with the caller.
Tests must only clear uniquely named test Stores, never flush shared Redis.

## Verification

- RED/GREEN tests reproduce checkpoint get/list timeout and interrupt behavior.
- Contract tests reject negative revisions and live items at revision zero.
- Real Valkey tests exercise two independent Redisson clients: shared reads,
  one winning conditional writer, stale updates/deletes, tombstone ABA protection,
  unconditional contention, namespace isolation, JSON round-trip, search/list,
  scoped clear, and version overflow without corruption.
- Legacy Core/Extensions Redis serialization compatibility tests remain green.
- Full Core tests, relevant/full Extensions tests, Checkstyle, formatting, lint,
  license, secret, source and binary compatibility evidence are collected.

## Deferred Boundary

Graph checkpoint CAS requires atomic snapshot loading and version checks on node
writes, manual updateState, release, and ReactAgent cancellation rewind. It is
not partially enabled in this batch. Execution leases, fencing, automatic Worker
recovery, tool receipts, and durable scheduling remain later milestones.
