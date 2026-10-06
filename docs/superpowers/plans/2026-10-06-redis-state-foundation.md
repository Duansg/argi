# Redis State Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Deliver real shared Redis Store/CAS and explicit checkpoint-read failures.

**Architecture:** Core adds optional Store-version contracts without changing
legacy interfaces. Extensions adds a Redisson-backed implementation using one
Redis Hash and atomic envelope replacement. Existing RedisSaver formats remain.

**Tech Stack:** Java 17, Maven, JUnit 5, Mockito, existing Redisson 3.52.0,
existing Testcontainers, Valkey 8.1.2.

**Spec:** `docs/superpowers/specs/2026-10-06-redis-state-foundation-design.md`

## Global Constraints

- Java 17; no new production dependencies.
- Keep existing public APIs/defaults/coordinates and checkpoint bytes/keys.
- Real Redis driver code stays in Extensions, not Core.
- No MQ, checkpoint-runtime CAS, execution leases, or Worker recovery in this batch.
- Do not revert user edits, merge main, push, or create PRs.
- All commits use Signed-off-by.
- Use real Redis tests for shared-state claims and no Redis flush commands.

## Task 1: Core Read Failure And Versioned Store Contracts

**Files:**
- Modify `argi-graph-core/src/main/java/io/github/agentic/ai/graph/checkpoint/savers/redis/RedisSaver.java`.
- Create `argi-graph-core/src/main/java/io/github/agentic/ai/graph/store/VersionedStore.java`.
- Create `argi-graph-core/src/main/java/io/github/agentic/ai/graph/store/VersionedStoreItem.java`.
- Create `argi-graph-core/src/test/java/io/github/agentic/ai/graph/checkpoint/savers/RedisSaverReadFailureTest.java`.
- Create `argi-graph-core/src/test/java/io/github/agentic/ai/graph/store/VersionedStoreItemTest.java`.

**Interfaces:**
- Consumes existing Store, StoreItem, RunnableConfig, BaseCheckpointSaver.
- Produces the three signatures in the spec and
  `record VersionedStoreItem(Optional<StoreItem> item, long version)`.

- [ ] Write RED tests for actual RedisSaver get/list with a mocked external RLock
  returning false, and interrupted lock acquisition. Assert exceptions rather
  than mock invocation counts. Include a real graph resume test that verifies
  no node executes when checkpoint read fails. Missing metadata remains empty.

```java
assertThrows(IllegalStateException.class, () -> saver.get(config));
assertThrows(IllegalStateException.class, () -> saver.list(config));
assertEquals(0, executedNodes.get());
```

- [ ] Run `./mvnw -B -ntp -pl :argi-graph-core -am
  -Dtest=RedisSaverReadFailureTest -Dsurefire.failIfNoSpecifiedTests=false test`.
  Expected RED: no exception on contention; preserve logs.
- [ ] Make timeout fail explicitly and restore Thread interrupt flags on
  interrupted reads. Do not change successful reads, write/release behavior,
  keys, or bytes.
- [ ] Test and implement additive contracts. Validation examples:

```java
assertThrows(IllegalArgumentException.class,
    () -> new VersionedStoreItem(Optional.empty(), -1));
assertThrows(IllegalArgumentException.class,
    () -> new VersionedStoreItem(Optional.of(item), 0));
new VersionedStoreItem(Optional.empty(), 3); // tombstone is valid
```

- [ ] Run focused tests and full Core suite; review diff and signed-off commit.
- [ ] Leader reviews the task and installs candidate Core into the isolated
  Maven repo `/tmp/argi-runtime-compat-m2.9nrvx3` before Task 2 starts.

## Task 2: Real Redis Store And Maintained Saver Failure Semantics

**Worktree:** `/Users/aias/Work/github/argi-extensions-redis-foundation`.

**Files:**
- Modify `graph-persistence/argi-graph-persistence-redis/src/main/java/io/github/agentic/ai/graph/persistence/redis/RedisSaver.java`.
- Create `graph-persistence/argi-graph-persistence-redis/src/main/java/io/github/agentic/ai/graph/persistence/redis/RedisStore.java`.
- Create matching `RedisStoreTest.java` and `RedisSaverReadFailureTest.java` in
  that module's existing test package.
- Create `graph-persistence/argi-graph-persistence-redis/README.md`.

**Interfaces:**
- Consumes Core VersionedStore/VersionedStoreItem from Task 1, plus BaseStore.
- Produces `RedisStore(RedissonClient)` and
  `RedisStore(RedissonClient, String storageKey)` implementing VersionedStore.
- Default Hash key: `argi:store:items:v1`. Tests use a random unique Hash key.

- [ ] Run maintained saver RED tests before changing its failure branches,
  using the same observable assertions as Task 1. Preserve serialization.
- [ ] Write RED real-Valkey tests for the new Store. Testcontainers must bind
  ephemeral ports, use Valkey 8.1.2, and create two independent clients.

```java
RedisStore a = new RedisStore(firstClient, uniqueKey);
RedisStore b = new RedisStore(secondClient, uniqueKey);
assertTrue(a.putItemIfVersion(item, 0));
assertEquals(1, b.getVersionedItem(namespace, key).version());
assertFalse(b.putItemIfVersion(otherItem, 0));
assertTrue(b.deleteItemIfVersion(namespace, key, 1));
assertEquals(2, a.getVersionedItem(namespace, key).version());
assertFalse(a.putItemIfVersion(item, 0));
assertTrue(a.putItemIfVersion(item, 2));
```

- [ ] Implement atomic JSON-envelope CAS with existing Redisson primitives or
  one-key Lua; no separated payload/version reads or local lock safety claims.
  Bound ordinary mutation retries; reject negative expected versions; fail on
  counter exhaustion before mutation. Clear/delete retain tombstone revisions.
- [ ] Extend tests for exactly one of two conditional writers winning, ordinary
  contended updates, search/filter/sort/pagination, prefix namespace listing,
  separate Store keys, nested business JSON markers, scoped clear, client
  reconstruction, and an injected Long.MAX_VALUE revision without corruption.
- [ ] Run focused module tests and existing Redis compatibility tests:
  `mvn -B -ntp -Dmaven.repo.local=/tmp/argi-runtime-compat-m2.9nrvx3
  -pl :argi-graph-persistence-redis -am test`.
  Expected GREEN with real Redis tests not skipped.
- [ ] Document explicit adoption, unchanged legacy classes, CAS semantics,
  persistent tombstones, non-transactional cross-item scans/clear, and the lack
  of Worker/graph checkpoint recovery guarantees. Signed-off commit.

## Task 3: Integrated Verification And Independent Review

- [ ] Independently review both task diffs for spec compliance and quality.
- [ ] Run full Core tests, Checkstyle/Spotless, lint, licenses, and diff checks.
- [ ] Install candidate Core into the isolated Maven repository and run full
  Extensions tests against it, not published/stale Core artifacts.
- [ ] Run genuine binary/source compatibility scripts from the previously
  reviewed compatibility-gate branch against this candidate; do not treat the
  current main Make target's skip messages as evidence.
- [ ] Verify old Core/Extension Redis readers remain byte-compatible.
- [ ] Report exact completed capabilities, real Redis validation, remaining
  checkpoint CAS/lease/recovery work, local branch names and no remote writes.
