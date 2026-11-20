# ClickHouse live sync and native S3 backup — staged, not cut over

## Deployment status

- Source lives in **better-sync**, not better-compact.
- Staged loopback receiver: `better-sync-clickhouse.service`, port **4176**.
- Existing receiver **4175**, production clients/feeders and legacy services
  remain unchanged. No source archive or historical exception was deleted.
- Persistent runtime: `/pg-hot/data/better-sync/runtime/`; spool on `/pg-hot`.
- The pilot uses a synthetic Pi source, isolated config/state/outbox under
  `/pg-hot/data/better-sync/pilot-*`. Initial upload, append and unchanged-file
  pass succeeded through the actual CLI and native TLS ClickHouse connection.
- Runtime uses a previously explicitly pinned private TLS certificate; system
  trust was not modified. Replace this with an administrator-trusted CA for
  production. `CLICKHOUSE_ALLOW_PINNED_LEAF=1` is explicit opt-in only.
- Backup and S3 READ/WRITE grants are now confirmed. The native full →
  incremental → isolated restore drill **passed**, including restored SHA256
  checks. The first real database backup is running under
  `better-sync-backup.service`; the hourly timer is enabled with weekly new
  full bases. A backup failure/recovery-point-age watch is armed. Production
  ingestion cutover remains gated on the remaining verification/projection work.

## Storage/acknowledgement contract

`sync.store="clickhouse"` selects the committed-segment HTTP protocol while
retaining the legacy `transport="s3"` HTTP adapter name for configuration
compatibility. Old configs and old receiver behavior are unchanged. The endpoint
must advertise ClickHouse protocol 1 and a persistent authority ID; a 404 is not
accepted as a ClickHouse destination. A producer persistently pins that identity
on its first sync, and refuses a different authority or incompatible pre-pin
namespaced state. Read-only status, verify and targeted retry require the existing
pin. The authority is provisioned once and must be restored with the database. A replacement database must never invent the old
ID without restoring its data.

The producer writes each at-most-8-MiB immutable segment to its durable local
outbox: payload fsync, atomic rename, directory fsync, then descriptor fsync and
rename. A response is accepted only if it is a ClickHouse commit receipt matching
authority, source, file identity, path, byte range, full/append marker and checksum.
Unacknowledged descriptors survive source deletion and are drained before scanning
new data. A validated ClickHouse receipt atomically marks the descriptor committed
but does **not** delete its payload: single-server insert acknowledgement is not
proof of backup durability. All committed bytes remain retained and count against
`sync.max_outbox_bytes`; reaching the limit stops new uploads rather than deleting
unverified recovery copies. No automatic release exists yet because independently
verified backup inclusion of the segment, references and chunks is not yet
implemented. Each segment is snapshotted before its send; this is not a promise
that an entire changing source file was frozen atomically before the first segment.
Local source deletion is not enabled by this path.

Local SQLite acceleration is namespaced by authority ID, so old S3 'sent'
checkpoints cannot skip an initial ClickHouse backfill. Existing source identities
and file version/append planning remain intact. Sync status reads the matching
state file and reports pending and retained durable outbox segments and bytes.
`sync retry-s3` discovers the configured ClickHouse authority and clears retries
only in its namespaced state; receiver discovery failures fail closed.

The authenticated receiver validates length/ranges/metadata and body SHA256
before inserting data. It preserves raw bytes, including partial trailing records
and binary content. Content-addressed 256-KiB chunks deduplicate bytes; ordered
references preserve every occurrence. This is a raw sync store, **not** semantic
event-level deduplication or a normalized event-query service. Rewrites get new
file-version IDs from existing planning and remain reconstructible; identical text
at distinct byte positions is not erased.

Chunks and references are acknowledged first; `sync_segments` receipts are
inserted last. Reads use FINAL because ReplacingMergeTree uniqueness is eventual.
One coordinated receiver writer serializes immutable-identity conflict checking;
do not run multiple independent instances against the same namespace without
coordination. A different payload/metadata under an existing identity returns 409.

Tables: `sync_refs`, `sync_segments`, `sync_authority`; blobs are configurable.
The pilot reuses `archive_migration_v1_chunks` to avoid duplicating imported bytes.
All tables inherit the operator-confirmed default tiered policy. No new global
storage policy/TTL was imposed. Native synchronous insert acknowledgement is not
an assertion of fsync or replicated quorum: the actual single-server persistence
and outbox-release durability policy still needs an explicit deployment decision.

## Native compressed backup

`backup.py` is a local single-flight trigger for native `BACKUP DATABASE ... TO
S3(...) ASYNC`. It does not re-export logs. Native backups retain the compressed
ZSTD table parts plus metadata. Incremental backups refer to the prior successful
backup; only completed jobs advance the local recovery-point state. A new full
base is chosen weekly. Merges can rewrite parts and increase uploaded bytes.
Confirmed terminal destination-conflict failures (native code 598) get at most
three attempts with backoff and fresh UUID/prefixes; failed prefixes are kept
and only the last successful backup is used as an incremental base. Unknown or
running outcomes are never treated as retryable terminal conflicts.
A synchronous canary reproduced the false conflict: the lock PUT response timed
out, its conditional retry received PreconditionFailed, and ClickHouse labeled
its own existing lock as a competing writer. Backup jobs now disable adaptive
S3 timeouts and request 120-second request / 5-second connection timeouts; lock
protection is not disabled. However, engine-settings readback shows this native
backup engine overrides request_timeout_ms to 3,600,000: those query settings
must NOT be presented as a proven fix. Six follow-up canaries passed but still
contained retried S3 response timeouts. The underlying lost-acknowledgement bug
can recur; terminal conflict retries remain bounded and failed prefixes retained.
Upstream BackupImpl.cpp now has explicit own-lock readback handling after a
conditional PUT commits but its acknowledgement fails; deploying a server build
with that handling is the robust fix, not deleting locks or unbounded retries.

A chosen backup ID is persisted **before** SQL submission. If acknowledgement is
lost, the scheduler must locate that ID in system.backups before another backup
can start. Unknown/failed active jobs require inspection, not automatic state
clearing. The local scheduler state is backup infrastructure, not another ingestion
ledger. Losing it must not cause old backups to be deleted; repository recovery
or a fresh full backup is needed. No automatic retention is implemented: never
expire a base while dependent incrementals are retained. `backup_retention.py`
provides a fail-closed, dependency-aware **dry-run planner only**: it never
performs deletion, protects every full base, the two newest chains and any
restore-verified chain, and returns no candidates for incomplete repository
inventory, unknown prefixes/jobs, cycles or missing dependencies. The inventory
of known successful prefixes alone cannot prove that unknown descendants do not
exist, so it is not permission to delete.

Required administrator grants for the configured account:

```sql
GRANT BACKUP ON session_center.* TO session_center;
GRANT SELECT ON system.backups TO session_center;
-- ClickHouse 26.8 also checks data-source access for native S3 backups/restores.
GRANT READ, WRITE ON S3 TO session_center;
```

Existing CREATE TABLE/INSERT/DROP rights in this database are used for isolated
restore tables. Server-side S3 endpoint access/configuration may require separate
administrator setup; privileges alone do not prove restore compatibility.

Environment (protected file, mode 0600):

- Existing CLICKHOUSE_HOST/PORT/USER/PASSWORD/DATABASE.
- CLICKHOUSE_CA; optionally CLICKHOUSE_ALLOW_PINNED_LEAF=1 for the explicit pin.
- S3_ACCESS_KEY/S3_SECRET_KEY scoped to the **new backup prefix**.
- CLICKHOUSE_BACKUP_S3_URL: complete endpoint/bucket/new-prefix URL, separate
  from historical `files/` and other archive namespaces.
- CLICKHOUSE_BACKUP_ALLOW_HTTP=1 only with an explicit private-network decision;
  otherwise require HTTPS. Do not expose secrets in shell arguments/logs.

After grants/configuration, run `restore_drill.py` in a supervised job. It creates
unique canaries, takes a full backup, inserts more bytes, takes an incremental,
restores to a separate table and compares SHA256 values. It never overwrites live
or imported tables; a failed/uncertain drill leaves isolated tables for inspection.
Canary backup objects remain retained. `restore_lineage_drill.py` additionally
checks a synthetic authority, replacement/append lineage, segment references,
chunk bytes and occurrence-aware VCC compilation after a full/incremental
restore. `restore_production_metadata.py` restores only four small metadata tables
from an existing production backup and checks a retained lineage; it does not
restore chunk bytes or authorize outbox release. `verify_chain_coverage.py`
schedules that check: it restores the metadata tables from the latest successful
production backup and compares every committed retained generation intent.
`cohort_restore_drill.py` backs up
and restores a tiny isolated copy of one actual ClickHouse lineage, including
its chunks; `cohort_proof_audit.py` compares that restored cohort with retained
local descriptors and bodies without deleting them. These pilots preserve their
scratch state and do not implement automated release or recovery. None proves
capacity for or recovery of a full production restore.
Record actual S3 uploaded bytes as part of
acceptance; no compressed-backup footprint has been measured yet.

Only then install/enable the `.service.example` and `.timer.example` templates,
trigger the first whole-database backup, verify the recovery point and arm backup
failure/age monitoring. A weekly full base and hourly incrementals target roughly
one-hour RPO, not zero loss after a single-server disk failure.

## Remaining cutover gates

1. Trusted TLS and explicit durability/outbox-release agreement.
2. Backup permission, full/incremental isolated restore drill, and first real
   database backup; scheduler enabled and monitored afterward.
3. Complete whole-file generation/coverage verification. The staged receiver
   exposes `/generation` and a `sync_generations` receipt table: one serialized
   writer checks explicit parent lineage, every contiguous segment, references,
   chunk hashes and each intermediate whole-file checksum before committing.
   `/verify` can reconstruct committed lineages and full replacements up to
   256 MiB. The staged producer now persists generation intents before segment
   uploads, validates generation receipts, and recovers checkpoint application
   from its durable outbox. A synthetic replacement → two appends produced
   committed/checkpointed intents and passed raw reconstruction. A supervised
   `sync run --once` completed successfully; earlier short probes timed out
   because the one-shot unnecessarily slept for the daemon poll interval, now
   corrected. At producer startup and every five minutes thereafter, applied
   generation receipts are checked against the pinned authority; lineage leaves
   are byte-verified (which also checks their intermediate hashes). Missing or
   invalid raw state triggers replay from retained bodies and re-verification.
   Unit tests simulate an
   older restore and missing references; a retained synthetic segment was
   replayed through the real loopback receiver. A real production backup
   restore and broader rollback remain unverified. Backup, loaded and indexed
   remain unknown; no production cutover is implied.
4. Complete operator controls beyond `retry-s3`, which is now authority-scoped.
5. Explicit live projection handoff: current VCC/search loaders consume old S3
   archives. `shadow_projection.py` can reconstruct a verified ClickHouse
   generation (currently capped at 256 MiB) into bounded temporary staging,
   then atomically index complete JSONL occurrences by
   authority/source/root-generation/byte-position in an
   isolated SQLite shadow. A synthetic pilot indexed seven events, including
   two equal-payload events at distinct positions; replay added zero events.
   Optional `--shadow-vcc` compiles a verified generation with the existing
   Codex/Pi compiler and atomically stores isolated block snapshots mapped to
   occurrence identities. Source kind is derived from committed segment metadata;
   ambiguous ordinals fail closed. Shadow compilation is capped at 8 MiB raw,
   50,000 events and 100,000 blocks; these are not hard RSS guarantees. A Pi
   replacement → append pilot matched direct compiler output, preserved two
   equal-payload messages as distinct, and replayed idempotently. The staged
   `better-sync-clickhouse-shadow.service` now scans committed generations in
   bounded pages, retries failures with backoff, and stores only an isolated
   local shadow (64-MiB aggregate storage backpressure proxy; 512-MiB service
   limit). Compiler and projection code are fingerprinted; code changes create
   a fresh shadow database while preserving and accounting for earlier shadows.
   Its initial staged sweep compiled nine synthetic generations without errors;
   after a restart, a new synthetic append was picked up and its five blocks
   matched direct compiler output. The shadow is not production VCC/search and
   advances no production checkpoint. `handoff_parity.py` performs a read-only,
   bounded ordered comparison against the exact legacy source/path; missing or
   ambiguous sessions are unknown. A match covers rendered block fields only,
   not source/archive coverage, timestamps, backups, or search. The separate
   `historical_frame_parity.py` checks one committed historical full frame; it
   does not establish that this is the latest generation. A real comparison
   found 25 frame blocks versus 224 in current legacy VCC, so it is a mismatch,
   not handoff evidence. Downstream consumers and the legacy handoff fence remain to be integrated before
   retiring the old flow.
6. Cut producers over in a bounded pilot, reconcile active tails, then switch
   the remaining hosts and drain old receiver spill. Preserve rollback config
   and all 432 historical exception records.

The receiver holds a crash-released host-local authority lock in addition to
its in-process writer mutex. This prevents a second receiver under the same OS
user on that host, but does **not** coordinate separate users or machines; a
distributed deployment still needs proven single-writer coordination.

No production cutover is claimed until these gates pass.

## Tests

```sh
python3 -m unittest discover -s scripts/clickhouse -p 'test_*.py'
bun test test/clickhouse-sync.test.ts test/s3-sync.test.ts test/cli.test.ts test/sync-verify.test.ts
bun run typecheck
```

The focused run passed 13 Python tests, 44 Bun tests and both TypeScript checks.
Native full/incremental backup and isolated restore semantics have also passed
against this server; evidence is in
`/pg-hot/data/better-sync/restore-drill/verified.json` and `run4.log`.
The drill exposed and corrected bare endpoint URL normalization; systemd-run
shell jobs must use --expand-environment=no when deriving URLs from sourced
variables, to prevent premature service-manager substitution.
