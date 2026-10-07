# Opt-In Execution Leases And Checkpoint Fencing

Status: implementation authorized by the user's 2026-10-06 continuation.

## Outcome And Boundaries

Add distributed admission and stale-owner checkpoint rejection to the delivered
checkpoint CAS capability. Keep Java 17, existing APIs/defaults/checkpoint formats
and all non-leased saver behavior. No new production dependency. No MQ, durable
work queue, automatic Worker recovery, tool receipt protocol or exactly-once claim.
No main merge, PR, remote push or artifact publication in this batch.

Select a new LeasedCheckpointSaver explicitly. Existing VersionedCheckpointSaver
and legacy savers keep their behavior. New Redis leased storage is separate from
both existing keyspaces. Switching existing conversations requires explicit
migration or a new namespace; no transparent read-through/migration is provided.

Cross-process admission fails fast when another active owner holds a namespace.
Existing JVM-local FIFO admission remains first, preserving its local behavior.
Redis acquisition/renewal/mutation errors fail closed, with no unleased fallback.
Redis 7+ or Valkey 8.1.2 is the supported scripted backend for this new mode.

## Contracts

Create LeasedCheckpointSaver next to VersionedCheckpointSaver; supporting types
live under checkpoint.lease. Exact signatures:

```java
public interface LeasedCheckpointSaver extends VersionedCheckpointSaver {
    LeaseOptions leaseOptions();
    ExecutionLease acquireLease(RunnableConfig config, UUID ownerId) throws Exception;
    ExecutionLease renewLease(RunnableConfig config, ExecutionLease lease) throws Exception;
    boolean releaseLease(RunnableConfig config, ExecutionLease lease) throws Exception;
    RunnableConfig putIfLeasedVersion(RunnableConfig config, Checkpoint checkpoint,
            long expectedRevision, ExecutionLease lease) throws Exception;
    Tag releaseIfLeasedVersion(RunnableConfig config, long expectedRevision,
            ExecutionLease lease) throws Exception;
}
public record LeaseOptions(Duration ttl, Duration heartbeatInterval) {}
public record ExecutionLease(String namespace, UUID ownerId, long fencingToken,
        long expiresAtMillis) {}
public interface ExecutionGuard {
    void assertActive();
    AutoCloseable onLoss(Runnable cancellation);
}
```

LeaseOptions.defaults() is TTL 30 seconds and heartbeat 10 seconds. Both durations
are positive whole milliseconds, heartbeat strictly below TTL, and TTL at most
one day. ExecutionLease validates non-null namespace/owner, nonempty namespace,
positive fencingToken and positive expiresAtMillis. Fence is a positive Java long.

LeaseBusyException, LeaseLostException and LeaseRequiredException are unchecked,
metadata-only exceptions under checkpoint.lease. Busy/required expose namespace;
lost exposes namespace, expected owner UUID and expected fence, with a fixed
reason and optional cause. No state payload or competing owner's identity.
Constructors: LeaseBusyException(String namespace),
LeaseRequiredException(String namespace), and
LeaseLostException(String namespace, UUID ownerId, long fencingToken, String reason)
plus the same lost constructor with Throwable cause as final parameter.
Getters: getNamespace for all; lost also getOwnerId/getFencingToken/getReason.

LeasedCheckpointSaver provides ownerless put/release/putIfVersion/releaseIfVersion
defaults that throw LeaseRequiredException. Providers do not override them to
silently acquire or retry. Reads (get/list/getVersioned) remain available without
a lease. Renew requires exact owner/fence and unexpired stored lease; it cannot
revive an expired lease. releaseLease removes ownership only on exact owner/fence
and returns false for absent/stale ownership, never clearing another owner.

Lease acquisition increments a retained namespace fence before returning authority
and never increments checkpoint revision. Renew/unlock never alter checkpoint
revision/history/fence. Checkpoint writes/releases require current active owner
AND fence AND expected revision in one atomic boundary. Release preserves empty
positive checkpoint revisions and retained fences. Neither counter resets/TTLs.

MemoryLeasedCheckpointSaver composes VersionedMemoryCheckpointSaver under one
monitor with a retained lease/fence map. Constructors: no-arg;
(StateSerializer, LeaseOptions); (StateSerializer, LeaseOptions, Clock).
Clock is meaningful backend time injection; no public test-only mutation hooks.
Compute overflow/expiry and serializer copies before mutations. This is only a
single-process reference, not distributed storage.

## Runtime Scope

ExecutionLeaseScope lives in Reactor Context, keyed by saver identity plus
checkpointThreadId(config), matching current nesting. API:

```java
static <T> Flux<T> withLease(LeasedCheckpointSaver saver, RunnableConfig config,
        Function<ExecutionLeaseScope, Flux<T>> operation);
static Optional<ExecutionLeaseScope> current(ContextView context,
        LeasedCheckpointSaver saver, RunnableConfig config);
ExecutionLease lease();
ExecutionGuard guard();
void assertActive();
void invalidate(Throwable cause);
```

withLease is cold per subscription, after JVM queue grant and before snapshot
load. Fresh acquisition uses UUID.randomUUID(); matching nested scope reuses one
owner, timer and cleanup. Different namespace has separate ownership. Scope
credentials are only in Reactor scope; its ExecutionGuard is an opaque adapter
that exposes no owner/fence/backend key and only liveness/cancellation.

Use monotonic elapsed time from successful RPC REQUEST START plus TTL, not JVM
wall clock or acknowledgement time. A slow acknowledgement that arrives beyond
the previous local validity window cannot restore authority. Loss is terminal.
Separate deadline watchdog and heartbeat prevent a blocked renewal from delaying
local loss detection. Renewals run off timer threads on boundedElastic; no
overlapping heartbeat RPCs per scope. Injectable package-private constructor
scheduler/time dependencies are used by the production factory and virtual-time
tests; no global mutable hooks or public test APIs.

Loss latches LeaseLostException, requests registered cooperative cancellation,
and cancels graph upstream while exposing a terminal typed error. Ensure normal
operation completion does not wait forever for a never-emitting loss publisher.
Cancel/deadline/error/success all dispose timers/listeners; cleanup is outermost
only. Outermost cancellation must run Agent rewind while a still-valid lease is
held, then release ownership. On loss, rewind is rejected, not retried. Cleanup
release is expected-owner best effort and must not mask cancellation/original
execution error; unconfirmed release expires naturally and never resumes work.
Observer/cancellation callback failures must not block loss of other callbacks.
The operation is the primary lifetime. A valid operator shape is
operation.takeUntilOther(lossSignal).concatWith(terminalLossOrEmpty): normal
completion cancels the watcher and completes, while loss cancels upstream and
then emits the latched LeaseLostException. Never merge a normally infinite loss
publisher into a success path. Cleanup belongs outside the entire operation
publisher, including its cancellation doFinally handlers, not an inner release.

## Core And Agent Integration

GraphRunner/ReactAgent/subgraph update-then-stream wrap leased execution in
withLease inside the existing queue, then open/reuse VersionedCheckpointScope.
Adapt the existing versioned scope rather than duplicating snapshot/history logic:
leased mode obtains a matching active ExecutionLeaseScope from Reactor context;
put/release/rewind call the leased mutation methods, never ownerless CAS. Missing
lease scope fails closed. Snapshot loading validates authority before and after
the read. Lease-related backend rejection invalidates scope; checkpoint conflicts
retain their existing terminal behavior. Heartbeat never advances owned revision.

Standalone CompiledGraph.updateState acquires a lease around snapshot/routing/CAS
and cleanup. The scoped child overload inherits its existing matching lease.
No reroute, node replay, tool replay or stale revision refresh on failure.

Add an opaque run-only ExecutionGuard field to RunnableConfig, with optional
executionGuard(), assertExecutionActive(), and Builder.executionGuard(...).
Copy it in builders; clearContext must not remove it. Mark field/accessor
JsonIgnore and transient; omit it from toString/metadata/state. This field is
liveness only, not lease credentials or revision ownership. Graph admission
clears incoming stale guards; GraphRunnerContext injects/rebinds the current
guard after fresh admission, including resume/setConfig/checkpoint config copies.
Public updateState and StateSnapshot configs clear it before returning. Original
caller configs are not mutated. Cold resubscriptions get fresh guards.
RunnableConfig.withoutExecutionGuard() is the concrete sanitizer (return this
when absent, otherwise builder-copy with executionGuard(null)). Use it at fresh
graph/Agent admission, in both StateSnapshot.of overloads, stateOf/getStateHistory
snapshot return boundaries, and standalone public updateState returns. The
scoped/internal updateState overload keeps runtime binding until child admission.
GraphRunnerContext has a private bindExecutionGuard(config) helper used after
start/resume initialization, setConfig and checkpoint config replacement.

GraphRunnerContext.assertExecutionActive and NodeExecutor checks run before user
listeners/interruption/actions, and before merging late results. Existing
stream/parallel paths also guard dispatch/late checkpoint writes through context.

AgentToolNode checks guard before batches, after permit acquisition, before each
leaf dispatch (also after interceptors) and before returning/merging results.
LeaseLostException is not converted to a ToolCallResponse error or swallowed by
generic tool exception processors; post-interceptor/batch checks prevent a
processor from masking loss. Registered cancellation requests cancel supported
tokens and completes a framework-owned awaited future exceptionally; do not wait
indefinitely for an uncooperative original future. Close registrations in finally.
Non-leased paths keep their existing timeout/error behavior.

Add AsyncToolCallbackAdapter.callAsync(arguments, ToolContext, ExecutionGuard).
It checks the guard inside the executor callable, so queued wrapped sync tools
cannot start after known loss. Existing constructors/methods remain unchanged;
AgentToolNode uses this overload for adapters, including pre-created adapters.
Running sync/external callbacks are not forcibly stopped or rolled back.

Leased operator nesting is exactly JVM queue -> ExecutionLeaseScope.withLease ->
VersionedCheckpointScope.withScope -> graph stream + Agent cancellation rewind
doFinally. ExecutionLeaseScope releases after that whole inner operation's cancel
path. VersionedCheckpointScope factory fetches the matching current lease before
constructing (or reusing) a leased scope; the constructor receives that scope and
guards getVersioned before/after. put/release/rewind dispatch leased methods with
leaseScope.lease(); no ownerless leased mutations remain. Backend lease rejection
invalidates ownership before propagating; state-version conflicts stay distinct.

## Redis Storage And Atomic Protocol

RedisLeasedCheckpointSaver builder: redisson(RedissonClient),
stateSerializer(StateSerializer), leaseOptions(LeaseOptions),
storageKeyPrefix(String). Default prefix argi:checkpoint:leased:v1.
Physical key: prefix + ":{" + Base64 URL without padding of UTF-8 resolved
checkpointThreadId(config) + "}". One hash/key per namespace. Fixed fields:
owner (UUID string), fence (decimal), expires (server epoch milliseconds),
revision (checkpoint decimal), history (existing Base64 CheckPointSerializer).
No whole-key TTL. Legacy and current versioned keyspaces remain untouched.

Use existing Redisson 3.52.0 RScript with StringCodec.INSTANCE and ReturnType.MULTI.
All calls, including atomic read-only snapshot scripts, use READ_WRITE routing
to the authoritative primary; replica reads after acquisition are not sufficient.
Every script gets exactly one explicit KEYS[1]. No dynamically accessed keys,
RLock/thread ownership, duplicate lease key or reuse of VersionedStore revision.

Lua uses Redis TIME for expiry, validates before writes (scripts do not roll back
prior mutations on errors), and validates malformed field combinations fail closed.
Fenced mutation compares owner/fence/revision as exact STRINGS. Java computes
Math.addExact(expectedRevision, 1) before RPC and passes decimal strings. Do not
parse revision/fence through Lua tonumber/cjson. Acquire uses native HINCRBY on
retained fence then HGET to return the exact string, not a Lua numeric reply.
Reject malformed/negative/noncanonical counters; detect Long.MAX_VALUE before
mutating, and preserve owner/history/counters on overflow. Expiry millisecond
arithmetic stays below 2^53; validate bounds before HINCRBY/HSET.

Return compact string status arrays (OK/BUSY/LOST/CONFLICT/CORRUPT/OVERFLOW) plus
decimal metadata, never history in exceptions. A vanished owner or expired lease
is LOST even if checkpoint revision still matches. A valid owner's stale revision
is CheckpointConflictException with latest observed diagnostics; no fallback.
Atomic snapshot returns revision+history together, rejects inconsistent pairs,
and preserves virgin empty revision zero and released empty positive revision.
Reuse CheckPointSerializer and existing retention behavior. Prepare released Tag
before commit and return only on known successful release. Clients are caller-owned.

Official evidence: Redis Eval atomicity/SCRIPT KILL/TIME/HINCRBY/Cluster docs and
Redisson 3.52.0 sources, recorded in /tmp/argi-execution-lease-reference.md.
This guarantee assumes one authoritative retained Redis history; asynchronous
HA failover/backup rollback is not a consensus/durable-fencing guarantee.

## Acceptance

- Active A excludes B before nodes/tools; queued cancellation performs no acquire.
- Expired A, then B with higher fence, rejects A write/release at unchanged revision.
- Wrong owner or wrong fence cannot renew/unlock/mutate; unlock cannot erase B.
- Renewals do not change checkpoint revision/history and expired renew cannot revive.
- Counter overflow and malformed data fail before writes; values above 2^53 round-trip.
- Initial/acquire/renew/mutation errors fail closed; no legacy/base-CAS fallback.
- Late renewal success cannot revive lost scope; deadline watchdog works during blocked RPC.
- Same namespace nesting acquires/releases/times once; other namespaces stay independent.
- Cancel rewinds before unlocking if valid, but never overwrites after loss/new owner.
- Guard survives builder/clearContext, is replaced on child admission, absent from
  public configs/serialization, and fresh across cold subscriptions.
- Sequential/parallel/queued adapters stop new calls on known loss; supported tokens
  are notified; late results are not merged/persisted. Real tool counters prove no replay.
- Actual Valkey with independent clients exercises whole graphs, manual updates,
  release/reuse, lease loss, retention/serializer, and unchanged older storage.
- Full Core/Extensions, genuine source/binary compatibility, lint/licenses/secrets,
  whitespace and independent reviews pass; all new backend tests execute without skips.
