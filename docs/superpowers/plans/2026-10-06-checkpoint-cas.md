# Checkpoint Execution CAS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans task-by-task.

**Goal:** Opt-in stale-checkpoint protection for every runtime mutation path.

**Architecture:** Core defines an explicit versioned saver and per-subscription
Reactor revision scope. Graph and Agent paths use it only for new saver types.
Extensions implements RedisVersionedSaver on the completed RedisStore backend.

**Tech Stack:** Java 17, Maven, JUnit 5, Mockito, Reactor, existing Redisson 3.52.0,
Testcontainers and Valkey 8.1.2.

**Spec:** docs/superpowers/specs/2026-10-06-checkpoint-cas-design.md

## Global Constraints

- Java 17 and existing APIs/defaults/graph order/legacy checkpoint formats remain.
- No new production dependency, MQ, leases, Worker recovery or exactly-once claim.
- Never retry/overwrite a stale runtime CAS after node/edge/tool side effects.
- No main merge, PR or remote push for this new batch; foundation push is complete.
- Preserve user files and use Signed-off-by commits and apply_patch edits.
- All mutation paths include release and cancellation rewind, not only node put.
- Test real behavior with deterministic latches/sinks and actual Valkey clients.

## Task 1: Saver Contracts And Memory Reference

**Files:** Create checkpoint/VersionedCheckpointSaver.java,
CheckpointSnapshot.java, CheckpointConflictException.java,
checkpoint/savers/VersionedMemoryCheckpointSaver.java and corresponding tests
under argi-graph-core/src/{main,test}/java/io/github/agentic/ai/graph.

**Interfaces:** Produces the exact saver/snapshot signatures in the spec.
Conflict exception constructor is (String namespace,long expectedRevision,long
actualRevision), exposing getNamespace/getExpectedRevision/getActualRevision.
Memory constructors are no-arg and (StateSerializer serializer).

- [ ] Write RED tests for snapshot validation and real memory revision outcomes:

```java
var saver = new VersionedMemoryCheckpointSaver();
assertEquals(0, saver.getVersioned(config).revision());
saver.putIfVersion(config, firstCheckpoint, 0);
assertEquals(1, saver.getVersioned(config).revision());
assertThrows(CheckpointConflictException.class,
    () -> saver.putIfVersion(config, otherCheckpoint, 0));
saver.releaseIfVersion(config, 1);
assertTrue(saver.getVersioned(config).checkpoint().isEmpty());
assertEquals(2, saver.getVersioned(config).revision());
```

- [ ] Run focused tests, preserving expected missing-feature RED evidence.
- [ ] Implement composition around MemorySaver with one mutation monitor and
  independent cloned snapshots, revisions retained after release. Exactly one
  increment per successful mutation; overflow before delegate mutation.
- [ ] Test selected checkpoint, replacement, retention, empty release, namespace
  isolation, negative revisions, snapshot mutation independence and two stale
  conditional writers (one wins). No public test-only hooks.
- [ ] Run graph-core module tests/static checks; signed-off commit and review.

## Task 2: Revision Scope And Core Runtime Paths

**Files:** Create checkpoint/VersionedCheckpointScope.java; modify GraphRunner,
GraphRunnerContext, CompiledGraph, executor/BaseGraphExecutor,
internal/node/SubCompiledGraphNodeAction; create focused runtime/scope tests.

**Interfaces:** Consumes Task1 contracts. Produces withScope/snapshot/preTurnSnapshot/
hasOwnMutation/put/release/rewind in the spec. Adds GraphRunnerContext constructor
overload accepting scope; CompiledGraph updateState overload accepting scope.
The parameterized snapshot(config) preserves pinned history and validates its
observed namespace revision against the scope's current owned revision.

- [ ] Write RED real-graph tests proving stale execution and manual update/release
  cannot replace a winning checkpoint, while legacy savers still behave normally.
  Use two identity-distinct test facades over one reference backend to bypass
  the JVM same-saver queue without bypassing actual saver CAS.

```java
assertThrows(CheckpointConflictException.class, () -> staleUpdate.get());
assertEquals("winner", backend.getVersioned(config).checkpoint().orElseThrow()
    .getState().get("result"));
assertEquals(1, routeCalls.get()); // failed update never re-routes
```

- [ ] Implement a lazy per-subscription Reactor scope map keyed by saver identity
  and resolved namespace. Own revisions advance only after successful CAS.
  Conflicts are terminal; no backend revision reread on rewind.
- [ ] Integrate initial snapshot/merge and node put, completion release, manual
  update and scoped child resume. Preserve legacy branches exactly.
- [ ] Verify cold subscriptions get fresh scopes, distinct namespaces stay
  independent, pinned history remains explicit, and child update advances its
  inherited scope before child initialization. Validate saver identity on scoped
  overloads, and clone state before reducers mutate it.
- [ ] Run scoped tests and full Core reactor; signed-off commit and review.

## Task 3: ReactAgent Cancellation Integration

**Files:** Modify argi-agent-framework/.../agent/ReactAgent.java; add
ReactAgentVersionedCheckpointTest.java with offline model fixtures.

**Interfaces:** Consumes Task2 withScope and scope.rewind. No old Agent APIs change.

- [ ] Write RED tests for cancellation after own checkpoint writes, external
  namespace advancement before cancel, cancellation before any owned write,
  cold re-subscription and pre-turn read failure. Real tools increment observable
  counters; models use sinks/latches without external API calls.

```java
subscription.dispose();
assertEquals("external", saver.getVersioned(config).checkpoint().orElseThrow()
    .getState().get("owner")); // stale rewind must not replace the external writer
```

- [ ] In versioned mode open/reuse the subscription scope after the local queue
  grant. Use its snapshot rather than swallowed pre-turn read errors; rewind
  only last known-owned revision, no retry on conflict. Legacy path unchanged.
- [ ] Run offline Agent tests plus existing queue/cancellation suites and full
  reactor; signed-off commit and review. Install candidate Core after review.

## Task 4: Redis Versioned Saver

**Worktree:** /Users/aias/Work/github/argi-extensions-checkpoint-cas.
**Files:** Create persistence/redis/RedisVersionedSaver.java and test in existing
Redis module; update module README. No POM/dependency changes.

**Interfaces:** Consumes reviewed Core capability and existing RedisStore.
Builder uses redisson, stateSerializer, optional storageKey methods; default
storage key argi:checkpoint:versioned:v1 and Store namespace List.of("checkpoints").
History value key is content, encoded with existing CheckPointSerializer.

- [ ] Write RED actual-Valkey tests with independent clients for revision load,
  conditional history append/update, release and reuse, contention, selected
  history/retention, graph invoke/stream/update/release and legacy key isolation.

```java
saverA.putIfVersion(config, firstCheckpoint, 0);
assertEquals(1, saverB.getVersioned(config).revision());
assertThrows(CheckpointConflictException.class,
    () -> saverB.releaseIfVersion(config, 0));
assertTrue(saverA.getVersioned(config).checkpoint().isPresent());
```

- [ ] Implement atomic versioned history on RedisStore, no duplicate Lua layer.
  Decode history at the expected revision, mutate once and conditional-store
  once. Conflict remains typed and never falls back. Release stores empty
  history through CAS even for virgin namespace. Default no TTL/reset.
- [ ] Plain Base put/release may bounded-retry for compatibility callers; test
  runtime paths never call that fallback. Keep caller client ownership and use
  unique test storage keys, never Redis flush commands.
- [ ] Run all Redis tests with no skips, full candidate-linked Extensions tests,
  static/lint/license checks; document explicit new-format adoption and no
  migration/failover/side-effect guarantee. Signed-off commit and review.

## Task 5: Integrated Completion

- [ ] Final cross-module/repository review; resolve findings before handoff.
- [ ] Fresh full Core clean install and Extensions clean test from isolated Maven
  repo /tmp/argi-runtime-compat-m2.9nrvx3. No stale published Core artifacts.
- [ ] Genuine binary/source compatibility scripts, static/lint/license/secrets
  and whitespace checks. Current main's skipped Make gates are not evidence.
- [ ] Record exact semantics and remaining lease/tool-receipt/Worker work, keep
  local codex/checkpoint-cas branches, and leave main/user files untouched.
