# Opt-In Checkpoint Execution CAS

Status: Next implementation batch authorized on 2026-10-06 after Redis foundation.

## Goal And Boundaries

Protect all versioned checkpoint mutations against stale namespace revisions:
node writes, manual state updates, completion release, and streaming cancellation
rewind. Keep existing Java 17 APIs, graph execution order, defaults, checkpoint
formats and legacy saver behavior. Do not add MQ, execution leases, automatic
Worker failover, or an exactly-once tool guarantee. No new production dependency.

Activation is explicit selection of a new VersionedCheckpointSaver implementation.
Existing saver types remain on the current path; no global mode flag or silent
CAS fallback is introduced. New storage is separate from legacy Redis keys.
This batch is local work on codex/checkpoint-cas; only the completed foundation
branches were authorized for push. Do not push this new batch automatically.

## Core Contracts

Add under io.github.agentic.ai.graph.checkpoint:

```java
public interface VersionedCheckpointSaver extends BaseCheckpointSaver {
    CheckpointSnapshot getVersioned(RunnableConfig config);
    RunnableConfig putIfVersion(RunnableConfig config, Checkpoint checkpoint,
            long expectedRevision) throws Exception;
    Tag releaseIfVersion(RunnableConfig config, long expectedRevision) throws Exception;
}
public record CheckpointSnapshot(Optional<Checkpoint> checkpoint, long revision) {}
```

Never-written namespaces are empty at revision zero. Live checkpoints have
positive revisions. Release advances once and preserves an empty positive-revision
tombstone, including release of an empty namespace. Every successful put/release
increments once; overflow fails before mutation. Negative expected revisions are
invalid. A mismatch throws CheckpointConflictException and changes nothing.
The exception exposes namespace, expectedRevision and actualRevision without
capturing state payloads. Snapshot state is a consistent, independently owned
read; providers must not return their mutable internal storage as the snapshot.
For a failed atomic compare, actualRevision is the latest observed revision,
not necessarily the revision at the failed comparison instant. If a diagnostic
read fails, use -1 (unknown) and attach that failure as suppressed; the typed
conflict remains terminal and never authorizes retry.

Provide VersionedMemoryCheckpointSaver in checkpoint.savers as the Core reference.
Compose the existing MemorySaver rather than overriding its final methods.
Serialize/clone snapshots and incoming checkpoints using existing StateSerializer
support; constructors accept the default or a supplied StateSerializer. Snapshot,
write and release are under one local monitor. Keep revisions after release.
This implementation is a single-process reference, not distributed storage.

## Per-Subscription Revision Scope

Add VersionedCheckpointScope in Core, carried in Reactor Context. Key it by
saver object identity plus saver.checkpointThreadId(config), matching the local
execution queue's namespace convention. Never put trackers into persisted graph
state, NodeOutput, RunnableConfig.context or serializable metadata.

```java
static <T> Flux<T> withScope(VersionedCheckpointSaver saver, RunnableConfig config,
        Function<VersionedCheckpointScope, Flux<T>> operation);
CheckpointSnapshot snapshot();
CheckpointSnapshot snapshot(RunnableConfig config) throws Exception;
CheckpointSnapshot preTurnSnapshot();
boolean hasOwnMutation();
RunnableConfig put(RunnableConfig config, Checkpoint checkpoint) throws Exception;
BaseCheckpointSaver.Tag release(RunnableConfig config) throws Exception;
void rewind(RunnableConfig config) throws Exception;
```

withScope is lazy per subscription. It reuses a matching inherited scope or
loads one atomic initial snapshot and installs a new scope. Cold resubscription
must create a fresh scope. Scope mutations are synchronized; advance revision
and current snapshot only after known successful mutations. Compute next revision
before mutation. A conflict is terminal for the scope; no reload/retry/overwrite.
The parameterized snapshot validates saver/namespace identity and preserves a
requested historical checkpoint. When it differs from the cached selection,
load that selection atomically and require its namespace revision to equal the
scope's owned current revision. Never substitute cached head for pinned history;
a mismatch is a terminal conflict, not a revision refresh.

Rewind is a no-op without an owned mutation. Otherwise it uses the copied pre-turn
checkpoint (or START/END empty state) and the last revision successfully written
by THIS scope. It never reloads the backend revision. Conflict skips the rewind;
another run's newer state remains intact. Ambiguous transport results do not
authorize a blind rewind or imply exactly-once effects.

## Runtime Integration

- GraphRunner starts/reuses the revision scope inside the existing checkpoint
  execution queue, after acquiring the per-subscription local slot.
- GraphRunnerContext receives the scope through an additive constructor overload;
  its existing constructor remains available. Versioned initialization loads
  state/revision together from the scope, not through a second independent get.
  Clone snapshot state before merging so custom reducers cannot mutate the
  retained pre-turn checkpoint. Preserve start versus resume behavior and inputs.
- GraphRunnerContext.addCheckpoint uses scope.put for versioned savers and the
  old path otherwise. START and all node/stream/parallel paths already converge
  here. Cancellation continues to wait against its checkpoint write monitor.
- Add a context release helper and route BaseGraphExecutor completion release
  through scope.release only in versioned mode.
- Existing CompiledGraph.updateState reads one snapshot and performs one CAS in
  versioned mode. Do not retry route calculations or user edge side effects.
- Add updateState(config,values,asNode,VersionedCheckpointScope) for the internal
  context-aware subgraph path. Validate saver/namespace identity. Update the
  scope's current snapshot/revision after success. A pinned historical checkpoint
  must be loaded consistently at the same observed namespace revision; do not
  silently replace it with current head state.
- SubCompiledGraphNodeAction wraps versioned child update-then-stream in withScope,
  so child initialization sees the checkpoint just committed by its resume update.
  Legacy branches and existing namespace rewriting remain unchanged.
- ReactAgent streaming reuses withScope around snapshot capture/graph stream.
  Versioned snapshot read failures propagate; never turn them into an empty
  pre-turn snapshot. Cancel rewinds through the last-own-revision scope, best
  effort without masking cancellation. The legacy path remains unchanged.

## Redis Implementation

Add RedisVersionedSaver in the existing Extensions Redis persistence module.
Reuse RedisStore/VersionedStore atomics; no separate Lua/client layer is needed.
Default dedicated storage key: argi:checkpoint:versioned:v1. Namespace history
is one StoreItem keyed by checkpointThreadId(config), with namespace
List.of("checkpoints") and one Base64 history value named content. Reuse the
existing CheckPointSerializer; old RedisSaver keys/content are not migrated.

getVersioned deserializes history from the same atomic Store envelope as its
revision, selecting the requested checkpoint or latest checkpoint. Conditional
put loads history at the expected revision, applies append/replacement and the
existing retention helper, then commits once through putItemIfVersion. False
becomes a typed conflict, never an unconditional fallback.

Release CAS stores empty serialized history, retaining a positive revision even
for a never-written namespace. Return the released historical Tag only after
successful commit. A new activation uses that revision, preventing ABA. No TTL
or revision reset is added. Plain put/release compatibility methods may use
bounded CAS retries, but graph execution never calls them in versioned mode.
Builders mirror existing RedisSaver style, accepting RedissonClient,
StateSerializer and an optional storageKey. Clients remain caller-owned.

## Acceptance Evidence

Reference tests cover revision validation, stale put/release, retention, release
and reuse, selected history, snapshot ownership, overflow and concurrent CAS.
Real graph tests cover normal invocation/stream, single initial snapshot,
start/resume input merge, two distinct saver facades sharing one backend,
manual update conflict without rerouting/retry, release conflict, subgraph
resume and cold subscriptions. Offline ReactAgent tests cover own-revision
rewind, no-own-write cancellation, external writer winning before cancellation,
snapshot-read failure and legacy checkpoint/cancellation behavior.

Actual Valkey tests use two independent clients for whole graph execution,
conditional history writes, stale state update/release, release/reuse, retention,
serializer round-trip and legacy key isolation. Use latches/sinks, not sleeps.
All new atomic-storage tests must run rather than be skipped for the local gate.
Run full Core/Extensions tests, genuine binary/source compatibility gates,
static/lint/license/secret checks and independent reviews before completion.
