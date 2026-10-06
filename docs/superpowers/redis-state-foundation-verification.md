# Redis State Foundation Verification

## Delivered Scope

- Additive Core `VersionedStore` and `VersionedStoreItem` contracts.
- Real Extensions Redis Store using one JSON envelope per Redis Hash field and
  native Redisson atomic conditional insert/replace.
- Conditional update/delete, monotonic versions, tombstone ABA protection,
  scoped namespace operations and clear, bounded ordinary retries, and overflow
  failure before mutation.
- Core and Extensions Redis checkpoint reads distinguish missing state from
  lock failure, restore interrupted-read flags, and avoid cleanup ownership
  queries when no lock was acquired.
- Existing Store defaults, checkpoint keys and serialized bytes remain available.

Core baseline: `e24b9988ae0be6126f2bf927ea94ff9b989691ee`.
Core production tip: `73ed1271f`.
Extensions baseline: `ec023a24910a0a30c0e3e1c810e4c3ac2ef0dc40`.
Extensions tip: `23202c97`.
Both worktrees use local branch `codex/redis-state-foundation`.
No main merge, remote push, PR, or new production dependency was performed.
The verified Extensions branch is also preserved in the independent persistent
clone `/Users/aias/Work/github/argi-extensions`, with the same commit and file
tree as the test worktree. Its Git object database does not depend on `/tmp`.

## Evidence

| Check | Observed result |
| --- | --- |
| Core full clean install | 1,358 tests; 0 failures/errors; 219 skipped; all 7 modules successful |
| Extensions full clean test against candidate Core | 413 tests; 0 failures/errors; 27 skipped; all 73 modules successful |
| Redis module | 18 tests; 0 failures/errors/skips |
| Real Valkey coverage | 6 Store tests, 4 legacy serialization tests, 1 lock contention test |
| Read-failure and DTO contracts | Mock external RLock failure paths plus actual graph resume node-execution assertions |
| Binary compatibility | All 5 public/protected runtime modules compatible with ARGI baseline |
| Source compatibility | Existing 3-file API consumer compiles against candidate artifacts |
| Static checks | Checkstyle, Spotless, lint and license checks passed |
| Secrets and whitespace | No leaks in new commit ranges; diff checks clean |

The genuine compatibility scripts from the previously reviewed compatibility
branch were run with this candidate worktree as the working directory. Current
main's Make gate skip messages were not counted as compatibility evidence.

Verification commands:

```shell
./mvnw -B -ntp -Dmaven.repo.local=/tmp/argi-runtime-compat-m2.9nrvx3 clean install
mvn -B -ntp -Dmaven.repo.local=/tmp/argi-runtime-compat-m2.9nrvx3 clean test
mvn -B -ntp -Dmaven.repo.local=/tmp/argi-runtime-compat-m2.9nrvx3 \
  -pl :argi-graph-persistence-redis -am test
make lint licenses-check
```

Full logs are retained under `/tmp/argi-redis-foundation.4lBe6p`.
Test-only Valkey containers used ephemeral ports on the local OrbStack daemon;
no shared Redis flush operation was used.

## Review And TDD

Core read contention: RED 6 tests/5 failures, then GREEN. Additional cleanup
masking regression: RED 10 tests/4 failures, then GREEN.
Maintained saver contention: RED 6 tests/5 failures, then GREEN. Cleanup masking:
RED 8 tests/2 failures, then GREEN.
New Store behavior: RED 6 tests/6 failures against a compiling missing-feature
stub, then GREEN against actual Valkey.

Independent task reviews and final cross-repository review found one interrupted
cleanup issue and one private-test identifier spelling issue. Both were fixed,
covered by fresh validation, and independently re-reviewed. Final scoped review
approved with no remaining findings.

## Decisions And Remaining Work

1. Preserve the legacy Core Redis-like Store defaults and provide production
   Redis Store in Extensions. Applications must explicitly import the new class
   and use a matching Core build; there is no silent storage migration.
2. Defer checkpoint-executor CAS as one complete integration slice. Normal node
   writes alone are insufficient: manual updates, release and cancellation
   rewind must be version-protected too. This delays execution integration but
   avoids falsely enabling partial concurrency protection.

This batch does not implement execution leases/fencing, Redis-backed run
dispatch, automatic Worker failover, durable tool receipts, durable scheduling,
or MQ. Redis restart/failover durability and multi-process recovery have not been
validated. Cross-item scans and clear are not snapshot transactions, and
persistent tombstone reclamation remains a later retention/generation design.
