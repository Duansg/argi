# Execution Lease Verification

## Status And Scope

Local opt-in execution leases and checkpoint fencing, following
`specs/2026-10-06-execution-leases-design.md`. Core contracts, lifecycle and
Agent dispatch and Redis implementation have passed task reviews. Final
cross-repository review is complete, with no Critical, Important or Minor
findings. The batch is verified for local handoff only.

Core candidate: `a58321ae9`, baseline `da17359cf`.
Extensions candidate: `ed078357`, baseline `b81e3516`.
Both branches: `codex/redis-execution-leases`.
No main merge, remote push, PR or registry publication.

Environment: Java 17.0.3, Maven 3.9.16, Docker server 29.4.0.
Every integrated Maven invocation uses the isolated local repository
`/tmp/argi-runtime-compat-m2.9nrvx3`.
Raw logs: `/tmp/argi-execution-leases.wYDFaV`.

## Core Dynamic Checks

Fresh full build:

```shell
./mvnw -B -Dmaven.repo.local=/tmp/argi-runtime-compat-m2.9nrvx3 clean install
```

`final-core-clean-install.log`: BUILD SUCCESS.

| Module | Tests | Failures | Errors | Skips |
| --- | ---: | ---: | ---: | ---: |
| Graph Core | 563 | 0 | 0 | 57 |
| Agent Framework | 723 | 0 | 0 | 162 |
| Studio | 23 | 0 | 0 | 0 |
| Graph Observation | 18 | 0 | 0 | 0 |
| Built-in Nodes | 111 | 0 | 0 | 0 |
| Total | 1438 | 0 | 0 | 219 |

The 219 skips also occur in the pre-change baseline. New lease tests are
offline and execute without skips. Focused Core lease tests: 33 passing;
focused Agent lease/adapter tests: 29 passing; adjacent Agent tests: 75 passing.
Behavioral RED logs demonstrate stale-clock renewal, premature local queue
handoff, elapsed deadlines, stale dispatch, interceptor masking and an
uncooperative awaited future before their respective fixes.

The installed Graph Core jar matches the candidate build byte-for-byte:

```text
713663195a259552009133a3fbaf0c7eab527d4a54f4af3a328c3c6d041a759c
```

## Compatibility And Hygiene

Actual scripts were executed, not the Makefile's skipped compatibility targets:

```shell
BINARY_COMPAT_MAVEN_REPO=/tmp/argi-runtime-compat-m2.9nrvx3 \
  bash tools/scripts/verify-core-binary-compatibility.sh da17359cf
SOURCE_COMPAT_MAVEN_REPO=/tmp/argi-runtime-compat-m2.9nrvx3 \
  bash tools/scripts/verify-core-source-compatibility.sh
```

All five runtime module binary checks passed against the approved ARGI baseline.
All three source-consumer fixtures compiled with Java 17.
Logs: `final-core-binary-compatibility.log` and
`final-core-source-compatibility.log`.

Core `make lint`, `make licenses-check`, and
`git diff --check da17359cf..HEAD` passed. Gitleaks scanned a `git archive HEAD`
snapshot with redacted output and reported no leaks; untracked user files and
ignored build/report artifacts were not included. Logs: `final-core-lint.log`,
`final-core-licenses.log`, `final-core-secrets.log`.
No meaningful Java LSP backend is available; Maven compilation is the Java
semantic check. No skipped placeholder is counted as validation.

## Redis And Extensions Checks

The backend is actual Valkey 8.1.2 with independently owned Redisson clients.
The new Redis lease suite runs 19 tests with zero skips. It covers busy admission
before nodes, expiry/reacquisition at unchanged revision, rejected stale writes,
retained int64 fence/revision counters above `2^53`, overflow/corruption without
mutation, renewal isolation, retention/serialization, old-key isolation,
ownerless mutation rejection, nested namespace ownership, cold subscriptions,
and cancellation rewind before unlock. Deterministic faults at the Redisson
script boundary cover acquire, initial snapshot, renewal, fenced put and release.
Mutation tests fail when fencing or leased-write fault invalidation is bypassed,
then pass when the production implementation is restored.

Final integrated build, using the reviewed installed Core candidate:

```shell
mvn -B -Dmaven.repo.local=/tmp/argi-runtime-compat-m2.9nrvx3 clean test
```

`final-extensions-clean-test.log`: BUILD SUCCESS, 445 tests, zero failures/errors,
27 existing baseline skips. All 19 new lease tests execute. Java format and
Checkstyle checks run in the Maven lifecycle. `make lint`, `make licenses-check`
and `git diff --check b81e3516..HEAD` pass.

Tracked-HEAD Gitleaks scanning reports one existing localhost TLS test fixture:
`memory-repository/argi-model-chat-memory-repository-redis/src/test/resources/ssl/key.pem`.
Its Git blob is unchanged from baseline:
`3c771d78342f0467219e94334ff1bd89c182e08b`. The test's generation instructions
and container configuration confirm fixture use. An independent baseline archive
scan followed by a candidate baseline-differential scan reports zero new leaks.
No blanket allowlist or scanner configuration changes were made. All scanner
output and JSON reports are redacted. Logs: `final-extensions-secrets.log`,
`extensions-baseline-secrets-relative.log`, `final-extensions-secrets-new.log`.

## Reviews

Each implementation task passed independent specification and quality review.
Core lifecycle findings repaired queue/unlock ordering, direct deadline checks,
callback registration, close/renew races and stale deadline callbacks. Redis
review added backend-specific cancellation/nesting/fault tests and exposed the
need to invalidate scope on leased RPC failure. Scoped re-reviews confirmed
the repairs and found no new blocking issues. Whole-branch review covered
33 changed files across Core and Extensions, checked the retained raw validation
logs and found no new issues. Both local feature branches and worktrees remain
available; no merge, push, PR or artifact publication was performed.

## Boundaries

- Existing non-leased APIs, defaults, savers and checkpoint formats stay unchanged.
- Redis leased storage uses a separate keyspace and requires explicit adoption;
  existing conversation migration is not automatic.
- Lease loss is terminal and prevents new dispatch and stale checkpoint commits.
  Running synchronous or external side effects are not forcibly rolled back.
- No MQ, durable Worker queue, automatic failover/resume, tool receipts or
  exactly-once execution is added.
- Redis fencing assumes one authoritative retained history. Asynchronous HA
  failover or backup rollback does not provide consensus-level fencing.

## Handoff

No implementation, verification or review task remains open in this batch.
Core runtime source candidate is `a58321ae9`; later Core commits only record
verification documentation. Extensions candidate is `ed078357`. The original
Core main checkout remains at `e24b9988a`, including its existing untracked files.
