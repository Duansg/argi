## Task 2 Report: Revision Scope And Core Runtime Paths

### Implemented

- Added `VersionedCheckpointScope` as a lazy per-subscription Reactor context scope keyed by saver identity and resolved checkpoint namespace.
- Routed versioned graph initialization, checkpoint writes, completion release, manual `CompiledGraph.updateState`, and subgraph resume update+stream through the scope.
- Preserved legacy saver branches for non-versioned savers.
- Hardened `CheckpointSnapshot` to reject a present checkpoint at revision zero.
- Carried forward `CheckpointConflictException` Javadocs clarifying `actualRevision` semantics.

### Files Changed

- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/checkpoint/VersionedCheckpointScope.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/GraphRunner.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/GraphRunnerContext.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/CompiledGraph.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/executor/BaseGraphExecutor.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/internal/node/SubCompiledGraphNodeAction.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/checkpoint/CheckpointSnapshot.java`
- `argi-graph-core/src/main/java/io/github/agentic/ai/graph/checkpoint/CheckpointConflictException.java`
- `argi-graph-core/src/test/java/io/github/agentic/ai/graph/checkpoint/VersionedCheckpointScopeIntegrationTest.java`
- `argi-graph-core/src/test/java/io/github/agentic/ai/graph/checkpoint/savers/VersionedMemoryCheckpointSaverTest.java`

### TDD Evidence

RED:

- `/tmp/argi-checkpoint-cas.JaMZMP/task2-red-snapshot-base.log`
- Command: `./mvnw -pl :argi-graph-core -Dtest=VersionedMemoryCheckpointSaverTest#snapshotRequiresOwnedOptionalAndNonNegativeRevision test`
- Base result: expected `IllegalArgumentException` for present checkpoint at revision zero, but nothing was thrown.

Additional RED during integration:

- `/tmp/argi-checkpoint-cas.JaMZMP/task2-red.log`
- Command: `./mvnw -pl :argi-graph-core -Dtest=VersionedMemoryCheckpointSaverTest,VersionedCheckpointScopeIntegrationTest test`
- Result: missing `VersionedCheckpointScope` compile failure before implementation.
- `/tmp/argi-checkpoint-cas.JaMZMP/task2-focused-2.log`
- Result: behavioral harness failure after first wiring pass; fixed by using identity-distinct legacy facades over the same backend.

GREEN:

- `/tmp/argi-checkpoint-cas.JaMZMP/task2-green-focused.log`
- Command: `./mvnw -pl :argi-graph-core -Dtest=VersionedMemoryCheckpointSaverTest,VersionedCheckpointScopeIntegrationTest test`
- Result: 18 tests, 0 failures, 0 errors, 0 skipped.

### Verification

- Focused checkpoint/scope: `./mvnw -pl :argi-graph-core -Dtest=VersionedMemoryCheckpointSaverTest,VersionedCheckpointScopeIntegrationTest test` -> 18 passing.
- Neighbor suites: `./mvnw -pl :argi-graph-core -Dtest=StateGraphConcurrentExecutionTest,CompiledSubGraphTest,GraphRunnerContextResumeWithoutCheckpointTest,GraphRunnerContextResumeMessageOrderTest,CheckpointExecutionQueueTest test` -> 28 passing.
- Full core: `./mvnw -pl :argi-graph-core test` -> 520 tests, 0 failures, 0 errors, 57 skipped.
- Hygiene: `git diff --check` -> clean.

### Self-Review

- Scope is not stored in `RunnableConfig`, context, metadata, node output, or graph state.
- Scope advances revisions only after successful CAS writes/releases.
- Scoped historical snapshot reads fail on observed revision mismatch instead of substituting head state.
- Versioned subgraph resume wraps update and stream in one child scope.
- No production dependencies added.

### Concerns

- Full core Maven emits existing deprecation/provider warnings, but all tests pass.
