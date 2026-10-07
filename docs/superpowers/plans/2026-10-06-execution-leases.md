# Execution Leases And Checkpoint Fencing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans task-by-task. Steps use checkbox syntax.

**Goal:** Opt-in distributed admission and atomic stale-owner checkpoint rejection.

**Architecture:** Add a leased saver contract/reference, a Reactor ownership scope,
and run-only dispatch guard. Redis uses one new namespace hash for lease metadata
and independently versioned checkpoint history, with server-side atomic fencing.

**Tech Stack:** Java 17, Maven, Reactor, JUnit 5/Mockito, existing Redisson 3.52.0,
Testcontainers Valkey 8.1.2; Redis 7+ for deployed scripted mode.

**Spec:** docs/superpowers/specs/2026-10-06-execution-leases-design.md

## Global Constraints

- Java 17; existing APIs/defaults/checkpoint formats and non-leased behavior remain.
- No new production dependency, MQ, durable queue, Worker recovery or exactly-once claim.
- Explicit new LeasedCheckpointSaver adoption; old keyspaces are not migrated/read through.
- Lease loss is terminal; no node/tool/routing replay or ownerless/stale CAS fallback.
- Owner/fence/active expiry/checkpoint revision checked in one atomic backend operation.
- Heartbeat never changes checkpoint revision; retained fences/revisions have no TTL/reset.
- No main merge, PR, remote push or artifact publication; preserve user files.
- Apply_patch edits, signed-off commits, ignored-only reports, deterministic tests.

## Task 1: Lease Contracts And Memory Reference

**Files:** Create argi-graph-core/src/main/java/io/github/agentic/ai/graph/checkpoint/
LeasedCheckpointSaver.java; checkpoint/lease/{LeaseOptions,ExecutionLease,
ExecutionGuard,LeaseBusyException,LeaseLostException,LeaseRequiredException}.java;
checkpoint/savers/MemoryLeasedCheckpointSaver.java. Corresponding tests under
argi-graph-core/src/test/java/io/github/agentic/ai/graph/checkpoint/lease and savers.

**Interfaces:** Exact signatures/constructors from spec Contracts. Owner UUID,
positive long fence, expiry milliseconds; defaults TTL30s/heartbeat10s, max TTL1day.
Consumes VersionedMemoryCheckpointSaver/CheckpointSnapshot/CheckPointSerializer.

- [ ] Write tests first for a real clock-controlled memory namespace:

```java
var first = saver.acquireLease(config, UUID.randomUUID());
assertThrows(LeaseBusyException.class,
    () -> saver.acquireLease(config, UUID.randomUUID()));
saver.putIfLeasedVersion(config, checkpoint("seed"), 0, first);
assertEquals(1, saver.getVersioned(config).revision());
clock.advance(Duration.ofSeconds(31));
var second = saver.acquireLease(config, UUID.randomUUID());
assertTrue(second.fencingToken() > first.fencingToken());
assertThrows(LeaseLostException.class,
    () -> saver.putIfLeasedVersion(config, checkpoint("stale"), 1, first));
assertEquals(1, saver.getVersioned(config).revision());
```

The mutable Clock and checkpoint fixture are test utilities, not production hooks.
- [ ] Focused RED: ./mvnw -B -pl :argi-graph-core -Dtest=MemoryLeasedCheckpointSaverTest,LeaseOptionsTest test.
- [ ] Implement composition under one monitor. Ownerless mutation defaults throw;
  acquire advances only fence, renew/unlock only owner timing, history operations
  delegate only while exact owner/fence is active. Overflow before mutation.
- [ ] Add negative/null/duration/namespace/wrong-owner/wrong-fence tests, lease
  expiry/reacquisition, renew-revision isolation, stale unlock, release positive
  tombstones/reuse, history selection/retention/ownership and serializer-failure
  atomicity. Fence/revision overflow seeded by private reflection only.
- [ ] Focused GREEN, full graph-core once, git diff --check, signed-off scoped commit.

## Task 2: Core Lease Lifecycle And Guard Plumbing

**Files:** Create checkpoint/lease/ExecutionLeaseScope.java and tests. Modify
RunnableConfig, GraphRunner, GraphRunnerContext, CompiledGraph,
checkpoint/VersionedCheckpointScope, executor/{NodeExecutor,MainGraphExecutor}
as dispatch needs require, internal/node/SubCompiledGraphNodeAction,
state/StateSnapshot. No Agent source edits in this task.

**Interfaces:** Consumes Task1. Produces withLease/current/lease/guard/assertActive/
invalidate API from spec. Adds RunnableConfig.executionGuard(),
assertExecutionActive(), Builder.executionGuard(ExecutionGuard); null clears it.
Adds GraphRunnerContext.assertExecutionActive(). Opaque guard is not credentials.
RunnableConfig.withoutExecutionGuard() is the concrete public sanitizer.

- [ ] RED real graph tests: second distinct saver facade sharing one memory
  backend gets Busy before node counter increments; expired owner fails fenced
  write at unchanged checkpoint revision; no-base-fallback facade throws from
  all ownerless writes.

```java
assertThrows(LeaseBusyException.class, () -> otherGraph.invoke(input, config));
assertEquals(0, otherNodeCalls.get());
assertEquals(beforeRevision, backend.getVersioned(config).revision());
```

- [ ] Implement cold scope after JVM grant and before versioned snapshot. Matching
  nested scope reuses ownership. Adapt VersionedCheckpointScope leased branch;
  do not duplicate its defensive snapshot/pin/revision logic.
- [ ] Bind leased VersionedCheckpointScope explicitly: withScope obtains
  ExecutionLeaseScope.current(context, leasedSaver, config), validates identity/
  namespace/active status, and passes it into its constructor. Guard BEFORE and
  AFTER getVersioned, including selected pin reads. Missing scope fails closed.
  All put/release/rewind branches call leased methods with leaseScope.lease();
  backend lease rejection invalidates scope before propagating, never base CAS.
- [ ] Implement heartbeat and independent monotonic deadline watchdog, latched
  loss, non-overlapping off-timer RPC, loss publisher cancellation/typed error,
  outermost cleanup. Use package-private dependency constructor used by the
  production factory and virtual-time fixtures; no global test flags.
- [ ] Normal completion must not await an infinite loss signal. Implement
  operation.takeUntilOther(lossSignal).concatWith(terminalLossOrEmpty), or an
  equivalent cancellation/error race, inside outermost resource cleanup:

```java
Flux<T> raced = operationFlux.takeUntilOther(lossSignal)
    .concatWith(Flux.defer(() -> terminalLoss == null
        ? Flux.empty() : Flux.error(terminalLoss)));
```

  terminalLoss is the scope's latched error. No-loss one-element operation must
  complete/release/dispose timers without time advancement; loss emits typed
  LeaseLostException, not completion or generic cancellation.
- [ ] Guard runtime config copies/resume/setConfig; original input untouched,
  public update/snapshot configs cleared, JsonIgnore/transient field. Check
  before callbacks/action and merging late result; old paths no-op.
- [ ] Implement bindExecutionGuard in GraphRunnerContext after initializeFromStart,
  initializeFromResume, setConfig and checkpoint config replacement. Sanitize
  fresh public entry configs withoutExecutionGuard, both StateSnapshot.of
  overloads, stateOf/getStateHistory snapshot configs and standalone public
  updateState return. Scoped child update retains runtime binding; child new
  namespace clears inherited guard before fresh lease binding.
- [ ] Test exact acquire/renew/release counts, namespace independence, normal
  complete/cancel/error timer cleanup, cancellation rewind while lease held,
  pre-snapshot errors, blocked renew watchdog, late ack cannot revive, terminal
  callbacks including throwing listener, cold subscriptions, child update-then-
  stream, guard builder/clearContext and serializer exclusion.
- [ ] Focused tests while iterating, full ROOT ./mvnw -B test once, signed-off commit.

## Task 3: Agent Dispatch And Cancellation

**Files:** Modify argi-agent-framework/.../agent/ReactAgent.java,
node/AgentToolNode.java, tool/AsyncToolCallbackAdapter.java; add offline
ReactAgentExecutionLeaseTest/AgentToolNodeExecutionLeaseTest/adapter tests.

**Interfaces:** Consumes Task2 guard and scope. Produces additive adapter
callAsync(String,ToolContext,ExecutionGuard); old constructors/factories untouched.

- [ ] RED real offline tools: controlled first tool loses ownership, second
  tool must never execute; queued wrapped sync adapter must not start after
  guard loss; active cancellable tool receives token, late output not persisted.

```java
assertEquals(1, firstToolCalls.get());
assertEquals(0, secondToolCalls.get());
assertEquals("winner", backend.getVersioned(config).checkpoint().orElseThrow()
    .getState().get("result"));
```

- [ ] ReactAgent opens lease before snapshot inside queue and keeps cancellation
  rewind inside lease lifetime. Original legacy/versioned-only branches remain.
- [ ] Use this exact operator nesting (rewind is INSIDE lease resource lifetime):

```java
CheckpointExecutionQueue.serialize(saver, config,
    () -> ExecutionLeaseScope.withLease(saver, config,
        lease -> VersionedCheckpointScope.withScope(saver, config, serializer,
            scope -> compiledGraph.stream(input, config)
                .doFinally(signal -> rewindOnCancel(signal, config, scope)))));
```

  rewindOnCancel is the existing best-effort versioned helper. Outermost release
  follows the entire inner cancellation path; assert actual mutation/unlock
  sequence, and that lost authority skips rewind rather than blind retry.
- [ ] Guard each leaf dispatch and post-interceptor/batch/merge. Preserve typed
  loss through catches/processors, including parallel tasks/permit waiters.
  Async awaited future is framework-owned: on loss completes with typed error,
  requests token/original future cancellation, closes listener in finally.
- [ ] Exact AgentToolNode guards: apply entry, each sequential iteration, each
  parallel task body, after semaphore acquisition, before request/interceptor
  execution, in base handler immediately before leaf dispatch, after interceptor
  return, and before ordered response/state merge. Re-throw LeaseLostException
  from sync/async catches and parallel exceptional completion; unwrap it from
  CompletionException and never pass it to processors/extractErrorMessage or
  ToolCallResponse.error. Post-chain/batch checks catch interceptor masking.
- [ ] Adapter overload checks inside executor runnable; also guard pre-created
  adapters. Do not invent cancellability for running sync/external work.
- [ ] Verify wrapped/unwrapped sync, async, cancellable, sequential/parallel,
  interceptor masking, cold subscriptions, queued cancellation, live rewind
  before unlock, lost-owner skip rewind, no extra initial state reads and legacy
  timeout/cancellation suites.
- [ ] Full ROOT tests once, signed-off commit and review; root installs candidate
  Core into isolated Maven repo only after reviewed Task3.

## Task 4: Redis Leased Saver

**Worktree:** /Users/aias/Work/github/argi-extensions-execution-leases.
**Files:** New graph-persistence/argi-graph-persistence-redis/.../RedisLeasedCheckpointSaver.java,
RedisLeasedCheckpointSaverTest.java and Redis module README. Existing saver/store
implementations and POM/dependencies unchanged; reuse CheckPointSerializer.

**Interfaces:** Task1 SPI and reviewed Core runtime. Builder names/signatures,
key prefix, field names and atomic protocol exactly as spec. Existing mvn, no
wrapper; use -Dmaven.repo.local=/tmp/argi-runtime-compat-m2.9nrvx3 for EVERY build.

- [ ] RED real Valkey independent-client tests for busy admission and stale
  owner after expiry/reacquisition BEFORE checkpoint revision changes:

```java
var oldLease = saverA.acquireLease(config, UUID.randomUUID());
saverA.putIfLeasedVersion(config, checkpoint("seed"), 0, oldLease);
expireTestLeaseInUniqueHash();
var winner = saverB.acquireLease(config, UUID.randomUUID());
assertEquals(1, saverB.getVersioned(config).revision());
assertThrows(LeaseLostException.class,
    () -> saverA.releaseIfLeasedVersion(config, 1, oldLease));
assertFalse(saverA.releaseLease(config, oldLease));
```

The expiry helper changes only the test's unique hash expiry field; no flush.
- [ ] Implement single-key Lua acquire/renew/unlock/read/fenced-put/release;
  validate before writes, active owner/fence/revision atomic, server TIME,
  decimal-string int64 counters, native HINCRBY+HGET exact fence, primary routing.
- [ ] Acquire HGETs/validates the existing fence BEFORE HINCRBY: absent means0,
  canonical nonnegative decimal within [0,9223372036854775806] may increment,
  Long.MAX_VALUE returns OVERFLOW without mutation; malformed/negative/out-of-
  range return CORRUPT. Validate expiry/TTL bounds first. HINCRBY overflow itself
  is non-mutating, but post-increment validation of other fields is too late.
  Return HGET exact fence bulk string; never Lua numeric/cjson fence/revision.
- [ ] getVersioned uses one-key atomic snapshot validation script with
  RScript.Mode.READ_WRITE and ReturnType.MULTI to route to primary even though
  the script only reads. Acquire -> active validation -> primary snapshot ->
  active validation must precede any listeners/nodes. No replica getAll shortcut.
- [ ] Test 2^53+ fence/revision and Long.MAX_VALUE overflow, malformed field pairs,
  wrong owner/fence/namespace, empty release/reuse, renew-only timing, non-revival,
  client ownership, serializer/retention/history, atomic snapshot and all older
  keyspaces unchanged. Runtime ownerless guard spies must throw if called.
- [ ] Whole graphs on two independent clients: Busy before nodes/tools, loss
  during node with newer owner at unchanged revision, manual update/release,
  nested namespaces, live cancellation rewind and cold scopes. Simulate Redis
  failures deterministically at transport boundary, never skip new atomic tests.
- [ ] Record meaningful behavioral RED/mutation evidence and raw logs. All Redis
  tests zero skips, full Extensions root clean test once; lint/license/secrets
  and signed-off commit. README explicit migration/HA/side-effect limitations.

## Task 5: Integrated Verification And Handoff

- [ ] Independent whole-branch cross-repository review, address findings.
- [ ] Fresh Core clean install + Extensions clean test in isolated repository;
  installed graph-core SHA matches candidate and all new tests execute.
- [ ] Genuine five-module binary check and three-file Java17 source consumer
  scripts from runtime-reliability-fixes, not skipped Make placeholders.
- [ ] Both repo lint/licenses/tracked-HEAD secrets/whitespace plus Maven checks;
  retain raw stdout and exact totals/skips in verification doc.
- [ ] Keep local codex/redis-execution-leases branches/worktrees, no main merge,
  remote push or registry publication; user main/untracked files preserved.
