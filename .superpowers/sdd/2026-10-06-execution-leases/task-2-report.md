# Task 2 Report: Core Lease Lifecycle And Guard Plumbing

## Status

DONE.

## Implemented

- Added `ExecutionLeaseScope` as the per-subscription Reactor-context lease owner.
  - Acquires cold, after backend grant and before leased versioned snapshot reads.
  - Reuses matching nested scopes by saver identity plus checkpoint namespace.
  - Maintains a heartbeat loop, independent monotonic local deadline watchdog, latched typed lease loss, cancellation/error race, and outermost best-effort cleanup.
  - Exposes `withLease`, `current`, `lease`, `guard`, `assertActive`, and `invalidate`.
- Wired leased graph execution:
  - `GraphRunner` wraps leased savers in `ExecutionLeaseScope.withLease(...)` before `VersionedCheckpointScope.withScope(...)`.
  - `VersionedCheckpointScope` fails closed when a leased saver has no current `ExecutionLeaseScope`, guards snapshot reads before/after, and routes `put`, `release`, and `rewind` through leased methods.
  - `CompiledGraph.updateState` and subgraph update-then-stream execution acquire leases for leased savers and sanitize public configs.
- Added runtime-only guard plumbing:
  - `RunnableConfig.executionGuard()`, `assertExecutionActive()`, `withoutExecutionGuard()`, and `Builder.executionGuard(...)`.
  - `executionGuard` is `transient` and `@JsonIgnore`; `withoutExecutionGuard()` is the public sanitizer.
  - Builder copy and `clearContext()` preserve the runtime guard.
  - `StateSnapshot` and public state/update APIs clear guards from returned configs.
- Bound guards into dispatch:
  - `GraphRunnerContext` binds the current scope guard after initialization, resume/setConfig, and checkpoint config replacement.
  - Core dispatch checks guard activity before listeners, node actions, interrupt feedback handling, state merges, output checkpointing, and end/start dispatch.

## TDD Evidence

RED command:

```shell
./mvnw -B -pl :argi-graph-core -Dtest=ExecutionLeaseScopeCoreIntegrationTest test > /tmp/argi-execution-leases.wYDFaV/task2-red-2.log 2>&1
```

RED result: failed as expected while Task2 production API/behavior was missing. Log path:

```text
/tmp/argi-execution-leases.wYDFaV/task2-red-2.log
```

Focused GREEN command:

```shell
./mvnw -B -pl :argi-graph-core -Dtest=ExecutionLeaseScopeCoreIntegrationTest test > /tmp/argi-execution-leases.wYDFaV/task2-green-focus-expanded-5.log 2>&1
```

Focused GREEN result:

```text
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Adjacent GREEN command:

```shell
./mvnw -B -pl :argi-graph-core -Dtest=ExecutionLeaseScopeCoreIntegrationTest,MemoryLeasedCheckpointSaverTest,LeaseOptionsTest test > /tmp/argi-execution-leases.wYDFaV/task2-green-adjacent-expanded-2.log 2>&1
```

Adjacent GREEN result:

```text
Tests run: 29, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Full ROOT command:

```shell
./mvnw -B test > /tmp/argi-execution-leases.wYDFaV/task2-full-root-final-expanded.log 2>&1
```

Full ROOT result:

```text
Tests run: 111, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

The same full-root log also records upstream reactor module summaries before the final module:

```text
ARGI Graph Core .................................... SUCCESS
ARGI Agent Framework ............................... SUCCESS
ARGI Studio ........................................ SUCCESS
ARGI Starter Graph Observation ..................... SUCCESS
ARGI Graph Nodes ................................... SUCCESS
```

## Verification

Diff hygiene:

```shell
git diff --no-ext-diff --check
```

Result: passed with no output.

Debug leftover scan over added lines:

```shell
git diff --unified=0 | grep -nE '^\+.*(System\.out|TODO|FIXME|Thread\.sleep|debugger|breakpoint)' || true
```

Result: no matches.

Modified-file leftover scan:

```shell
grep -nE 'System\.out|TODO|FIXME|Thread\.sleep|debugger|breakpoint' <modified-files>
```

Result: only pre-existing `FIXME` comments were reported in `CompiledGraph.java` and `GraphRunnerContext.java`; no new debug leftovers were added.

Diagnostics:

```shell
lsp_servers
```

Result: no Java LSP backend is available in this harness; exposed diagnostics are TypeScript/grep only. Java diagnostics are covered by Maven compile/test evidence above. The exposed `lsp_diagnostics` command returned `diagnosticCount: 0` for the new production and test Java files, but it invoked `npx tsc --noEmit`, so it is not counted as Java semantic diagnostics.

## Files Changed

- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/CompiledGraph.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/GraphRunner.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/GraphRunnerContext.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/RunnableConfig.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/checkpoint/VersionedCheckpointScope.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/checkpoint/lease/ExecutionLeaseScope.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/executor/MainGraphExecutor.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/executor/NodeExecutor.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/internal/node/SubCompiledGraphNodeAction.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/state/StateSnapshot.java`
- `argi-graph-core/src/test/java/io/github/agentic/ai/graph/checkpoint/lease/ExecutionLeaseScopeCoreIntegrationTest.java`

## Self-Review

- Scope stayed within Task 2: no Agent source, Redis source, POM, or dependency edits.
- The core lease lifecycle is centralized in `ExecutionLeaseScope`; `VersionedCheckpointScope` keeps existing snapshot/revision/conflict behavior and only switches mutations to leased calls when a leased saver is active.
- Public configs are sanitized at state-history, state lookup, standalone update, and snapshot boundaries, while runtime copies keep the guard for internal dispatch.
- Existing non-leased versioned savers continue through the previous optimistic version path.
- `git status --porcelain -uall` showed only Task2 source/test/report paths after this report was added.

## Concerns

- No unresolved blockers.
- Java LSP diagnostics are unavailable from the exposed diagnostics backend; Maven compile/test is the semantic verification source.
- Some Task2 brief scenarios are covered indirectly by the end-to-end graph and saver tests rather than each having a one-test-per-bullet fixture. The focused Task2 tests explicitly cover busy-before-node, stale-owner fenced-write rejection, normal completion, cancellation cleanup while lease is held, loss cleanup rejection, matching nested scope reuse, independent namespace scopes, cold subscriptions, callback failure isolation, local deadline while renew is blocked, late renewal acknowledgement not reviving a lost scope, heartbeat revision isolation, pre-snapshot read failure before node action, leased subgraph update-then-stream, runtime guard copy/sanitization, and snapshot sanitizer behavior.
