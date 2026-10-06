# Checkpoint Execution CAS Verification

Status: complete. Implementation, verification gates and independent final
cross-repository review passed. Both checkpoint-CAS branches remain local.

## Delivery Boundary

The completed Redis foundation branches were pushed and remote SHAs checked:

- Core `codex/redis-state-foundation`: `b2619d9026aeaa565a6555911af29ba14e2f95ef`.
- Extensions `codex/redis-state-foundation`: `23202c974aada823bcea96e807b0eab30e3200f0`.

This subsequent batch remains local on `codex/checkpoint-cas` in both repositories.
No main merge, PR, publish or automatic remote push is part of this batch.

## Adoption And Semantics

Select a `VersionedCheckpointSaver` explicitly. Legacy savers keep their current
behavior and formats; no global flag or silent CAS fallback is introduced.
The memory implementation is a single-process reference, not distributed storage.

Each cold subscription obtains a revision scope after local queue admission.
Matching nested execution reuses the same saver-identity/namespace scope through
Reactor context. Revision trackers are not persisted in business state or config.
Runtime snapshots use the configured graph serializer and defensive copies.

Node writes (including START), manual state updates, completion release, scoped
child resume and Agent cancellation rewind use owned-revision CAS. A conflict
terminates the scope; it does not reread ownership, reroute, rerun nodes/tools,
blind-write or automatically retry the stale operation.

Release preserves an empty positive-revision tombstone, including virgin release,
so release/reuse cannot reset revisions to zero. Cancellation without own writes
does nothing. Otherwise rewind compares the last known own successful revision
and restores the copied pre-turn checkpoint, or a START/END empty-state checkpoint.
A newer external writer is preserved when that rewind conflicts.

Pinned historical state must be read at the scope's observed namespace revision;
it is never silently replaced with cached head state. Conflict actualRevision is
diagnostic latest-observed information, or -1 if its diagnostic read failed.

The Redis implementation uses its own `argi:checkpoint:versioned:v1` hash,
Store namespace `checkpoints`, and one checkpoint history value `content` per
resolved checkpoint thread. It reuses RedisStore atomic envelopes. Legacy Redis
keys are neither migrated nor reset; clients remain application-owned. No TTL or
revision reset is introduced.

## Evidence

| Gate | Result |
| --- | --- |
| Core contracts/reference | Independent review approved; 13 focused tests, zero failures/skips |
| Core scope/runtime | Scoped review approved; 28 focused tests, zero failures/skips |
| Agent cancellation | Scoped review approved; 14 focused tests, zero failures/skips; no-own-write guard mutation caught |
| Candidate-linked Redis/Valkey | Scoped review approved; 13 new tests and all 31 Redis module tests, zero failures/skips; two independent clients |
| Fresh full Core/Extensions | Core 1,394 tests / 219 skipped; Extensions 426 tests / 27 skipped across 73 modules; zero failures/errors |
| Genuine binary/source compatibility | All 5 runtime modules binary-compatible with main e24b9988a; 3-file legacy consumer compiles at Java 17 |
| Lint/license/static/secret/whitespace | Both repositories passed Maven checks, lint, licenses, tracked-HEAD gitleaks scans and diff whitespace checks |
| Final whole-branch review | Approved; no Critical, Important or Minor findings |

Logs are retained at `/tmp/argi-checkpoint-cas.JaMZMP`. Candidate linking and
compatibility use isolated Maven repository `/tmp/argi-runtime-compat-m2.9nrvx3`.
Java compilation evidence comes from Maven, not the TypeScript diagnostics backend.
Core clean install ran 1,394 tests, zero failures, 219 existing skipped cases.
Its installed graph-core JAR matched the build JAR at candidate linking time.
SHA256: `de6b4d06944789073a36c708aefe4141e1dcd3527c0fd60449e3bb35638f745c`.
Core lint/licenses and gitleaks on a tracked-HEAD archive passed. Source
compatibility used `/tmp/argi-checkpoint-cas-source-m2.ZUAG8A`, a separate clone
to avoid replacing artifacts while Extensions builds against the candidate.
Raw final logs are `final-core-clean-install.log` and `task4-fix1-root.log`.
New CAS tests all ran; existing skips require optional external services/model
credentials. Temporary guard/fallback mutations failed and were restored before
final passing tests; none are retained in the commits.
The final reviewer also resolved the deferred Agent-specific serializer-fixture
question: Core configured-serializer coverage plus the explicit Agent call is
sufficient for this batch. No unresolved findings remain.

## Remaining Distributed Runtime Work

CAS protects checkpoint state from stale mutation; it is not exclusive execution
ownership. Concurrent workers may execute node or tool side effects before one
loses its checkpoint CAS. This batch supplies neither execution leases/fencing,
durable work claiming, automatic Worker failover, nor a general tool receipt or
exactly-once guarantee. Those require separate execution and idempotency policies.

## Recorded Decisions

- Opt in by new saver selection: compatibility is preserved, but adoption is explicit.
- Use transient Reactor scopes: copies/resume do not lose revision ownership, but
  runtime context plumbing is required.
- Rewind only last known own revision: prevents stale cancellation overwrite, but
  conflicting/ambiguous partial state may require explicit reconciliation.
- Limit this batch to checkpoint CAS: reviewable state protection, not Worker
  ownership or exactly-once side effects.
- Require pinned selection revision matching and diagnostic-only actualRevision:
  history selection can need an additional read; diagnostics cannot authorize retry.
- Add configured-serializer scope overload: defensive ownership honors custom
  graph states; convenience callers must select their custom serializer explicitly.
