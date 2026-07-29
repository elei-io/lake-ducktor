# Vision

## Promise

Attach LakeDucktor to a DuckLake and stop worrying about physical file
maintenance.

LakeDucktor reads the policy stored in DuckLake, diagnoses physical file
health, and invokes native DuckLake maintenance operations. It does not define
retention or layout policy and never edits DuckLake metadata directly.

## Control loop

```text
inspect → diagnose → claim → treat → verify → repeat
```

Treatments are limited to native operations for:

- flushing inlined data
- merging adjacent files
- rewriting delete-heavy files
- expiring snapshots
- cleaning obsolete files
- removing orphaned files

Work is derived from current lake state rather than a durable task queue.
Coordination state, when needed, is disposable and safe to lose.

## Scope

LakeDucktor owns:

- maintenance prioritization and admission
- CPU, memory, I/O, and concurrency limits
- retries and graceful shutdown
- health, metrics, and structured logs

LakeDucktor does not own:

- lake provisioning
- application transactions
- query serving or SQL proxying
- users, authentication, or authorization
- storage and retention policy
- custom compaction semantics

## Operation

LakeDucktor scales maintenance compute without introducing a control plane.
Workers derive work from the lake, coordinate with disposable lake-local
claims when necessary, and retain no authoritative service state.

## Safety

- DuckLake remains valid if LakeDucktor is stopped or removed.
- Native DuckLake operations are the only mutation mechanism.
- Unset destructive policy remains unset.
- Every work unit is resource-bounded where DuckLake permits it.
- Crashed work is rediscovered from the lake; expired claims are reusable.

## Further reading

- [Maintenance learnings](MAINTENANCE.md)
- [Scaling](SCALING.md)
