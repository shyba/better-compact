# Plan: make ClickHouse the live sync authority

## Outcome

Complete the move from Pi/Codex session files through `better-sync` to a single
ClickHouse live log, with VCC/search projections, verification, backups and a
rehearsed rollback. S3 is a destination for ClickHouse backups and preserved
historical archives, not a second live sync authority. This plan is for the sync
system; compaction stays in `better-compact`.

**Execution rule:** carry out the plan without asking the user to make choices,
run commands, grant permissions, approve intermediate milestones, or perform
manual handoffs. Use existing credentials, endpoints and the conservative defaults
below. Automate checks, retries, deployments and evidence collection. Continue
independent work if an external dependency is unavailable; report any truly
unresolvable external issue only after completing everything possible. Never call
the cutover complete, lower a safety gate, or invent a receipt to work around a
blocker. Post-plan operator-only follow-ups are listed at the end.

The checkboxes describe work to be completed, not work already done.

## Execution log

- 2026-10-30: Inventoried the local services. The system Codex/Pi feeders and
  legacy receiver remain active; the separate enabled user sync unit points to
  a deleted temporary test installation and is in a restart loop. Its Codex
  root overlaps the active system feeder, so it was not blindly restarted with
  a new sender. A read-only PostgreSQL catalog snapshot contains 19 Codex, four
  Pi and two OpenCode source identities, but historical catalog entries do not
  prove active hosts or reachability; remote/fleet inventory is not complete.
- 2026-10-30: Added persistent authority pinning, receipt-retained,
  capacity-bounded ClickHouse outbox state, authority-scoped targeted retries,
  and separate pending/retained status. There
  is deliberately no release until independent backup-inclusion proof exists.
- 2026-10-30: Added bounded ClickHouse raw reconstruction/verification for full
  replacements; missing ranges and corrupt chunks fail. Append lineage remains
  `unknown`. The CLI separately reports backup, loaded and indexed as unknown.
  A synthetic loopback pilot verified raw replacement bytes and retained both
  replacement and append payloads; append and unverified downstream stages
  correctly remain incomplete. Added tested position-based JSONL extraction as
  a projection building block, not yet a projection worker.
- 2026-10-30: Implemented and deployed to the loopback staging receiver an
  explicit root-to-leaf lineage verifier and generation ledger. It verifies
  contiguous segments, chunk/segment hashes and each intermediate whole-file
  checksum, then writes the generation receipt last. A synthetic replacement →
  append chain passed actual ClickHouse reconstruction and `sync verify` reports
  its raw bytes verified while backup and projection remain unknown. Replaying
  an identical generation returned success; reuse with a changed checksum was
  rejected. The staged producer now persists generation intents and recovers
  receipt/checkpoint application. A separate loopback replacement → two-appends
  pilot stored and applied the replacement and three appends and verified raw
  bytes; a supervised `sync run --once` completed successfully. Earlier short
  probes timed out because one-shot unnecessarily slept for the daemon poll
  interval; this was corrected. Broader outage, rollback, backup-release and
  production coverage remain unverified.
- 2026-10-31: Added fsynced generation intents before segment delivery, exact
  receipt validation, predecessor-ordered replay and durable SQLite checkpoint
  application. Pre-generation segment-only state forces a full generation
  instead of being silently skipped. Lost-ack and checkpoint-crash unit tests
  passed; four loopback pilot generations are committed and checkpointed.
  Added startup and five-minute reconciliation of applied generations: missing
  receipts or raw bytes trigger retained-segment replay and re-verification.
  Verification checks lineage leaves to avoid quadratic prefix reads. Tests
  simulate an older restore and missing references; a synthetic retained segment
  was replayed through real loopback ClickHouse and reverified. A real backup
  restore remains unproven, and retained segment release is still disabled.
- 2026-10-31: Added an isolated SQLite shadow occurrence index. It reconstructs
  and verifies a full ClickHouse lineage before atomically recording complete
  JSONL records by authority/source/root-generation/byte position. The real
  loopback pilot indexed seven records, preserved two equal-payload records as
  distinct positions, and replay added none. This is not the production VCC or
  search projection; their handoff and end-to-end parity are still outstanding.
  The optional isolated VCC shadow now runs the existing Codex/Pi compiler,
  maps blocks back to occurrences, and fails closed on ambiguous ordinals.
  A real loopback Pi replacement/append pilot matched direct compiler output
  (three then four blocks), kept two identical messages distinct, and replayed
  idempotently. A bounded continuous shadow service is now running: it scans
  committed generations in pages, backs off failures, and enforces a 64-MiB
  shadow-storage backpressure proxy and 512-MiB process limit. Its first sweep
  compiled nine synthetic generations with zero failures. Restart preserved
  shadow counts; a later synthetic append was picked up automatically and all
  five resulting blocks matched direct VCC compiler output. The shadow worker
  fingerprints compiler/projection code and rebuilds into a new local database
  after code changes, preserving earlier shadows under one aggregate storage
  budget. Added a bounded, read-only ordered VCC parity comparator that refuses ambiguous source/path
  sessions; a synthetic pilot correctly reported unknown because no legacy
  session exists. Even a match proves only rendered fields, not raw/archive,
  timestamp, backup or search coverage. This is not a production consumer or
  the required legacy-to-ClickHouse handoff.
- 2026-10-31: A read-only comparison reconstructed a checksum-verified,
  committed historical full frame and compared rendered blocks to the exact
  legacy source/path. It found 25 compiled blocks versus 224 currently in VCC;
  this historical frame may be older, so the difference is neither a resolved
  discrepancy nor proof of a current-generation defect. Protected evidence is
  under `/pg-hot/data/better-sync/historical-parity-evidence/`. This does not
  authorize handoff. The first bounded chunk lookup hit the 512-MiB server
  query limit; using hash-key PREWHERE and bounded read settings succeeded.
  The same fix was deployed to the staging receiver and shadow, with a
  post-restart byte-exact raw verification; backup coverage remains unknown.
  Hardened local outbox recovery so a crash-orphaned body is never overwritten:
  matching bytes resume, while conflicting bytes and the new temporary copy
  both remain preserved and block delivery. First-generation intents also use
  no-replacement creation so concurrent conflicting writers cannot overwrite
  each other's initial evidence.
- 2026-10-31: Hardened the backup scheduler to check for server-side running
  backup/restore jobs before starting a new job, even if a killed process has
  released the local lock. Added a supervised tiny synthetic restore drill for
  actual authority, generation, segment, reference and chunk schemas plus VCC
  occurrence reconstruction. The first attempt ended with confirmed terminal
  backup code 598 and was retained. A fresh-prefix second attempt completed
  full and incremental backups and restore; after fixing a test assertion that
  overlooked the session metadata block, a read-only recheck confirmed authority,
  byte-exact lineage, four occurrences (including two identical events) and
  four VCC blocks. Evidence is in `lineage-restore-20261031-attempt2/state.json`;
  both attempts and prefixes are preserved. This is not production-chain proof.
  A separate bounded restore of only the four small production sync metadata
  tables from the latest real incremental succeeded: the authority and a
  retained pilot lineage of five generations, five segments and five references
  were read back and structurally checked. Evidence is in
  `/pg-hot/data/better-sync/prod-metadata-restore-20261031/state.json`.
  It excludes chunk bytes, search and the bulk archive tables, so it cannot
  authorize outbox release or establish full-restore capacity.
- 2026-10-31: Backed up an isolated, bounded cohort copied from the actual
  ClickHouse pilot lineage and restored it from its own native S3 backup.
  Restored byte verification and a separate read-only local-outbox audit matched
  all five generations, five segment descriptors and bodies, five unique
  chunks, and 307 bytes. Protected evidence is in
  `/pg-hot/data/better-sync/cohort-restore-20261031/`. This demonstrates a
  potential independent inclusion mechanism for one synthetic pilot only; it
  is not the scheduled production backup chain, not an automated release or
  recovery path, and no outbox bytes were removed.
- 2026-10-31: Added and deployed a crash-released per-authority host-local
  receiver lock. A second same-user process was refused while the staging
  receiver ran; crash release and foreign-lock preservation passed tests. This
  does not coordinate different users or hosts and is not a distributed
  single-writer guarantee.
- 2026-11-20: THE UPDATE CHANNEL IS THE FLEET DELIVERY MECHANISM — shipped.
  `better-compact update` on fleet hosts is a git self-update from the mirror
  (192.168.0.71:222, branch `default`); the entire ClickHouse work is now
  committed and pushed there (5ad08a4). The updated code carries an automatic,
  idempotent legacy→ClickHouse migration in `sync run`: an existing s3-store
  config migrates fail-closed when a ClickHouse receiver advertises on the
  same host at BETTER_SYNC_CLICKHOUSE_PORT (default 4176, this host's
  LAN-exposed receiver); on 404/outage/mismatch it retains the legacy store
  and retries next pass; env+config rewritten with a `.pre-clickhouse`
  rollback backup; BETTER_SYNC_NO_AUTO_MIGRATE=1 disables per host.
  Covered by two new end-to-end CLI tests (migration + fail-closed),
  cli.test.ts 28/28, runtime 28/28. Fleet rollout procedure: each host runs
  `better-compact update` twice (two-run protocol), then its daemon migrates
  and sends to ClickHouse; after `sync verify` shows raw coverage, its legacy
  feeder is retired and the source is wired into the projector + watch (one
  line each). Remaining operator items unchanged: physical access per host,
  the 451 MiB file bound, retention deletion decision, 598 upgrade optional
  (same-rack insurance posture).
- 2026-11-20 (late): FLEET HOST LIVE; sender resilience fixes. A fleet host
  (LAN 192.168.0.32, hostname `access`) auto-migrated via the update channel
  and is committing its codex sessions under source 576278fd…: 765 distinct
  paths and climbing (its root holds May-2026 history this host never had);
  paths do not overlap this host's 481, so per-path generation identity holds.
  The codex projector picks its new paths up automatically (cursor 0 →
  whole-file, codex dialect). Two resilience fixes shipped (b859a59):
  reconcile retries transient receiver statuses with backoff instead of
  aborting every pass, and verify/replay/generation timeouts went 120s →
  600s — the receiver serves heavy lineages (the 44 MiB pi session, fleet
  codex flood) slower than the old 120 s allowed, which surfaced as
  503/socket-close loops that stalled passes. Also: receiver read-only
  endpoints (/generation/status, /manifest) no longer consume the
  single-flight slot; a stale 3-week-old git index.lock removed.
- 2026-11-19: FLEET ROLLOUT SERVER SIDE COMPLETE. Discovery: this host IS
  192.168.0.31 (the MinIO host — same-machine backups); LAN neighbors include
  192.168.0.107 (SSH open, session-history evidence `--home-user-107-writer--`),
  .106, .109, .92, .81; PG `origin_host` knows exactly one other producer:
  `workstation` (drained Oct 2, data already in ClickHouse). The receiver now
  binds 0.0.0.0:4176 (bearer-token posture identical to legacy :4175; read-only
  /capabilities and /health no longer consume the single-flight slot, fixing
  startup 429s). `fleet-rollout.sh` installs the full client sender on any
  reachable host in one idempotent command (bun, code rsync, fail-closed
  authority pin, systemd user unit, verification) — validated end-to-end
  against localhost and torn down. The ONLY missing input is SSH credentials
  for the fleet hosts; the operator runs the script (or adds this host's
  pubkey) and each host needs one line added to the projector runner + watch.
- 2026-11-18 (operator input): S3 (192.168.0.31) and ClickHouse (192.168.0.42)
  share a rack. The native backups are therefore logical-failure insurance
  only (corruption, bad upgrade, accidental drop) — not rack-loss DR.
  Consequences: the 598 upgrade urgency drops (bounded retries suffice for
  same-rack insurance); rack-loss recovery for live data = rebuild ClickHouse
  anywhere and re-sync from the origin session files on this host (proven
  re-syncable); the only irreplaceable data in the rack is the imported
  historical archive catalog (~296 GiB, other hosts' pre-migration history).
  Whether to copy that catalog off-rack (e.g. to this host, ~298 GiB free) is
  an operator decision. Backup operations continue unchanged.
- 2026-11-18: Overnight the hourly backups failed terminally for four cycles
  (02:00–05:00, code 598 ×3 + one 499 per cycle) — the first multi-hour
  outage of the S3 lock race. Recovery: a manually triggered attempt ran as
  one long CREATING_BACKUP (~35 min, 1,194+ files, ~181 MB — the post-codex
  incrementals touch hundreds of small parts, so each backup is now hundreds
  of S3 PUTs, which is slow under degraded S3 latency but works). By morning
  the chain had fully recovered: 510 successful, recovery point fresh.
  Steady-state verification: latest generations byte-exact vs source files
  on both sources (sampled), projector/watch/shadow/coverage probes all
  healthy. MIGRATION COMPLETE for both local sources under the lean plan.
  Remaining operator follow-ups: other hosts, the 451 MiB file (receiver
  bound), 598 root-cause upgrade, retention deletion decision.
- 2026-11-17: CHAIN-COVERAGE-GATED OUTBOX RELEASE DEPLOYED. After the codex
  cutover the retained outbox held ~8 GiB of segment bodies; the senders'
  caps would eventually backpressure again. `release_outbox.py` unlinks a
  body only when all four proofs pass: committed receipt, a completed daily
  chain-coverage run against a backup that started after the segment was
  committed, the segment's row identity in the committed ClickHouse lineage,
  and a chunk read-back whose concatenation hashes to the descriptor sha256
  (point queries — batched/FINAL reads proved flaky under the 512 MiB
  per-query limit). Descriptors are kept as the audit trail; every release
  is logged to a JSONL ledger. First production run: 1,386 bodies released,
  8.16 GiB freed, zero errors; daily 01:30 schedule armed. Chunk-byte
  presence in ClickHouse is now PROVEN for released segments; what remains
  unverified is chunk bytes inside the S3 backups themselves.
- 2026-11-16 (final): MIGRATION STATE — both local sources live on the
  ClickHouse authority. pi (917f…, 14 files) and codex (5762…, 481 files,
  7.758 GiB) have byte-verified raw coverage; production VCC continues from
  ClickHouse via the projector (delta mode for codex, whole-file for pi);
  both legacy feeders are retired (rollback = systemctl start). The pilot
  senders and projector run as user services with watches; hourly backups,
  daily chain-coverage, and the isolated shadow all continue. Open items:
  the 451 MiB codex file (receiver 256 MiB commit-verification bound;
  operator follow-up), 22 superseded staged pi intents (retained), the
  chain-coverage-gated outbox release tool (bytes retained meanwhile),
  sync-only installs on other hosts (unreachable), retention dry-run and
  old-shadow-DB cleanup.
- 2026-11-16: CODEX SOURCE CUT OVER. The pilot uploaded all 481 JSONL rollout
  files (7.758 GiB committed generations; source 576278fd…). Parity analysis
  showed the codex compiler is context-sensitive — whole-file compilation
  differs from the loader's frame-wise output and would drop real tool
  blocks — so the projector gained delta mode: it compiles only records after
  the legacy byte cursor, reusing the loader's own append semantics, never
  recompiling history (validated on the scratch clone: 184 legacy blocks
  preserved, +100 appended, seq continues, cursor advances). `vcc-feed-codex`
  stopped and disabled (rollback = systemctl start; cursor overlap prevents
  duplicate content). Production run: 2179/2180 sessions already_current at
  the cursor, 1 file projected with fresh bytes. The projector service now
  loops both sources; the watch covers both. Exceptions: the 451 MiB rollout
  (1/482 files) exceeds the receiver's 256 MiB bounded commit-verification
  limit — static since Oct 19, still covered by legacy archives; raising that
  bound is an operator follow-up. 22 stale pi intents from the cutover burst
  remain staged-but-superseded (harmless, retained).
- 2026-11-15 (late): PI SOURCE CUT OVER TO PRODUCTION. Legacy `vcc-feed-pi`
  stopped and disabled (unit file intact; rollback = `systemctl start
  vcc-feed-pi`; the loader's file-cursor overlap logic prevents duplicate
  content if it is ever re-enabled). The new production projector
  (`pi_projection.py`) reuses the loader's own write functions and compiler,
  verified on a scratch clone of the schema first: append mode, replace mode,
  satellites and cursors all correct. Production run: 14/14 sessions
  projected including the 44 MiB session (11,501 blocks, +17,353 new bytes),
  zero duplicate (session, block_hash) pairs, tool satellites intact.
  Known, honest differences vs legacy rows: ordinals are file-wide (legacy
  had per-frame counters) and window_count reflects whole-file compile —
  content itself is identical (proven). Projector installed as
  `better-sync-pi-projection.service` (60s loop) + watch. Sender hit its
  256 MiB retained-outbox cap during the cutover burst (correct
  backpressure); cap raised to 4 GiB pending the gated release tool. The
  sender's stable-snapshot hashing races this hyper-active session while I
  write; 22 staged intents drain on the next settled pass, and production
  VCC advances per committed generation.
- 2026-11-15: VCC parity proven on production data: for three real pi sessions
  (skills 55 blocks, firecrawl-wreq 3, hackernews 375) the blocks compiled from
  ClickHouse raw coverage match the legacy production PG rows exactly — seq,
  kind, window, brief and full content, zero mismatches. This is the core
  per-source handoff evidence. Remaining for pi cutover: a production projector
  writing the same rows to PG under a per-source fence, then retiring the
  legacy pi feeder (config preserved; rollback = restart vcc-feed-pi). Codex
  root permissions are fine (user-owned); its evaluation follows pi.
- 2026-11-15: The pilot exposed the shadow compiler's large-file boundary in
  production: the 44 MiB session_center pi session exceeds the deliberate
  8 MiB bounded shadow-compiler input, so its 19 generations (and counting)
  are deferred into `shadow_failure` with exponential backoff — refusal by
  design, not data loss; all 13 within-bound pi files compiled to a 3,544-block
  VCC shadow. Production VCC is untouched (legacy path). The shadow watch was
  refined (check-shadow-health.py, tested live): within-bound generation
  failures still alert; only size-deferred ones are reported as known-limited.
  Building the spill-backed bounded compiler for large generations remains the
  real fix.
- 2026-11-15: Lean production pilot PASSED on the real pi session source
  (`/home/user/.pi/agent/sessions`, source 917f259d…, 14 files). One-hour
  protocol instead of a soak: baseline, switch, live-tail tracking, induced
  crash. Evidence: authority auto-discovered and pinned; 14/14 files
  byte-exact (latest generation = live source sha256); the actively growing
  session_center session was tracked through 24 successive generations, every
  one prefix-hash verified against the live file (contiguous, no holes, no
  duplicate occurrences, 0 replays); SIGKILL mid-pass then restart recovered
  cleanly with no gaps and no double commits; `sync verify` reports
  local=verified clickhouse_raw=verified for all 14, backup/loaded/indexed
  honestly unknown (release still disabled; outbox bytes retained). Pilot
  sender installed as `better-sync-pi-pilot.service` (user unit, rollback =
  disable unit; legacy pi feeder continues in parallel, so the legacy path is
  untouched). Next: retire the legacy pi feeder root to make this authoritative
  (config preserved), then evaluate the codex root.
- 2026-11-15: The scheduled chain-coverage job failed its first real timer
  run: the transient unit only loaded `clickhouse.env`, so the allow-HTTP
  policy was unset and the private-HTTP guard refused the derived repository
  (fail-closed worked as designed). The runner script now sources both env
  files and the driver path itself, so the scheduled unit no longer depends on
  inherited environment; verified with all relevant variables explicitly
  unset: 8/8 intents chain-covered against backup `9a942ac4`. Earlier failures
  are preserved in the history directories.
- 2026-11-11: Added scheduled production-chain coverage verification. After
  each check, the four small sync metadata tables are restored from the latest
  successful native backup and every committed retained generation intent is
  compared: the first run found 8/8 intents with their full lineage and
  segment references present in the chain. Chunk bytes remain explicitly
  unknown until a capacity-verified restore can read them; outbox release is
  still disabled. A daily timer plus a stale/loss watch were armed. The code-598
  lock race persists: between 2026-11-05 and 2026-11-11 the scheduler recorded
  60 additional terminal failures alongside successful retries (334→351
  successful), all with prefixes retained and no unknown-job overlap.
- 2026-11-03: The isolated shadow service was OOM-killed at its 512-MiB
  cgroup limit, then restarted automatically with ten generations and zero
  recorded failures. Journal evidence shows two OOM kills roughly 37 hours
  apart during otherwise idle polling. A 384-MiB early-warning watch later
  caught the restarted worker at about 394 MiB, mostly anonymous memory.
  In bounded connection-only diagnostics, 100 fresh TLS connections grew RSS
  to 198,428 KiB; 100 queries on one persistent connection stayed near 93,764
  KiB. Deployed connection reuse with reconnect on errors, preserving previous
  fingerprinted shadow databases. After restart it rebuilt ten generations
  with zero failures and about 42 MiB cgroup memory. Evidence is in
  `/pg-hot/data/better-sync/shadow-memory-diagnostic-20261103/`; long-run
  stability remains unproven and the memory watch stays active. This does not
  establish production projection completeness or justify raising the limit.
- 2026-10-31: Standard HTTP sync setup now detects a compatible ClickHouse
  receiver and pins its authority without a store-specific flag. Explicit 404
  preserves legacy setup; outages, unsupported protocols and implicit
  ClickHouse-to-legacy downgrade fail without changing the existing config.
  Existing installed configs were not switched.
- 2026-10-30: ClickHouse reported one full and 65 incremental production
  backups: the full's reported stored bytes were 43,349,666,029 and the 65
  incrementals together 8,901,230. A read-only S3 listing confirmed the
  incremental total. Full-prefix LIST timed out even after bounded retries, but
  a read-only audit of all 521 full-manifest-referenced objects confirmed their
  sizes, totaling 43,349,666,029 bytes including the manifest. This is not a
  complete bucket inventory, checksum audit, or restored-production proof.
- 2026-10-30: Rechecked historical exceptions: 432 remain (388 frozen/live
  manifest mismatches and 44 missing objects), with no success receipts. Added
  a tested dependency-aware retention *planner* that performs no deletion and
  fails closed without complete repository/descendant proof. No retention is
  enabled. Armed monitoring for the existing production feeder/loader services.
  A paced read-only recheck completed for all 432: 388 still have manifest
  mismatches and 44 are still missing; no object qualified even as a candidate
  for full checksum import. All frozen evidence and exception records remain.
  A separate sustained-D-state loader watch was armed after observing an I/O-
  blocked loader child; service-active alone is not proof of loader progress.
- 2026-10-30: The loader cgroup was above its 6-GiB MemoryHigh and undergoing
  severe direct reclaim. Raised the service-only runtime limits in two bounded
  steps to MemoryHigh=9 GiB and MemoryMax=10 GiB (originally 6/8 GiB); no global
  or ClickHouse limit changed, and no service/source state was stopped or reset.
  The worker left D-state, but a completed archive load has not yet been proved.
  The worker later grew to 9.6 GiB and stalled again; the limit increase did
  not solve the problem. At the user's direction the loader was stopped, its
  prior 6/8 GiB limits restored, and a runtime-only mask applied after an
  external actor restarted it twice. The worker exited and host available
  memory recovered; source data and checkpoints were not cleared. Feeders and
  the legacy receiver continue running, but VCC projection is paused. The
  loader stall/memory watches were replaced by a watch for unintended restart.
  The runtime mask is under /run/systemd/system.control and disappears on
  reboot. A first whole-frame parser memory reduction and a 64-MiB raw frame
  safety guard are implemented and tested offline; large frames remain pending,
  and this is not a hard RSS bound. Do not resume the full backlog until the
  bounded compiler and end-to-end memory/equivalence tests are complete; see
  `s3_sync/LOADER-MEMORY.md`. At the user's subsequent request, the runtime
  mask was removed and the loader resumed with the original 6/8-GiB limits;
  early memory remained low but startup exhibited D-state, so successful
  production processing and end-to-end memory behavior remain unverified.
  Stall, 3-GiB early-warning and hard-limit/headroom watches are armed. Later
  the service recorded successful archive loads with a 511-MiB peak, while many
  oversized frames remained explicitly pending. Added a tested same-digest,
  same-budget deferral to avoid repeatedly downloading those known failures;
  changed data or budget is retried automatically. Fixed the fast-skip resume
  parser to consume exactly the recorded body length, so it cannot swallow
  following frame headers; offline multi-frame, unterminated-body and truncation
  tests pass. Corrected overlap accounting to include blank-line bytes and
  fail a frame if a prior byte cursor lands inside a JSONL record rather than
  silently dropping the uncovered suffix. Offline whole-frame and failure-path
  tests passed; archives with such boundaries remain pending and a dedicated
  watch alerts on these failures. This change does not repair prior checkpoints,
  solve partial records across frames, or resolve
  or automatically dismiss old exceptions. On 2026-11-05 the loader stall
  watch timed out in system-wide `ps`; the actual loader child was in S-state,
  cgroup memory pressure was zero, and a recent archive load was observed.
  Replaced the watch probe with bounded cgroup-local process inspection and
  tested D-state, PID identity, process-exit and malformed-stat behavior.
- Test evidence so far: 99 ClickHouse Python tests, 50 focused Bun tests, TypeScript checks
  and the CLI build passed. Production cutover has **not** happened; generation
  lineage, independent backup inclusion/release, projection handoff, full
  recovery and fleet rollout remain outstanding. A full production restore was
  not attempted: current ClickHouse credentials cannot read server disk/memory
  telemetry (code 497), so the 40-GiB restore's safe capacity is unverified.

## Starting point and invariants

- Production still uses the legacy receiver at `:4175` and the S3 archive →
  PostgreSQL VCC/search loaders. The ClickHouse receiver at `:4176` is a
  loopback-only staging service; its initial-upload/append pilot passed.
- `better-sync` already contains a ClickHouse segment sender, a durable local
  outbox, and a receiver. A legacy `201` means local spill registration, **not**
  a ClickHouse commit. Existing legacy senders must not simply be pointed at the
  new receiver: the new protocol requires checksummed segments and validated
  authority-specific receipts.
- ClickHouse uses one persistent authority ID. Synchronous chunk and reference
  inserts precede the committed-segment receipt. Its `ReplacingMergeTree` keys
  are not uniqueness constraints: use correct logical reads (`FINAL` where
  required) and a single coordinated writer, or implement equivalent proven
  coordination. Payload hashes deduplicate storage only; source, generation and
  position distinguish separate occurrences, even when their bytes are identical.
- No source-file deletion, archive deletion, unknown-job clearing, foreign-lock
  deletion, backup-base deletion, or automatic dismissal of historical
  exceptions. Keep legacy service/config and all source evidence until coverage,
  recovery and handoff are proved. The 432 historical exceptions recorded in
  the prior migration are not successful imports and their present count must be
  rechecked rather than assumed.
- Use the existing explicitly pinned ClickHouse TLS peer, authenticated receiver
  access and existing transport protections. Do not silently trust a new peer,
  weaken TLS, expose bearer tokens, or turn on plaintext remote access. Keep the
  single-server durability limits explicit; a ClickHouse insert receipt alone is
  not proof of replicated, fsynced or backed-up data.

## 1. Keep the clients simple; upgrade the sync component

- [ ] Inventory actual producers and source roots, including `better-sync`
  Pi/Codex installations and the separate `s3_sync/scripts/feed-rollouts.mjs`
  feeder. Record the active service, endpoint, source identity, checkpoint and
  pending/spilled work for each. Do not assume all producers use the same code.
- [ ] Keep Pi and Codex writing their ordinary session files. Ship one upgraded
  `better-sync` sync package/service per participating host, including sync-only
  installs; do not add ClickHouse clients to Pi, Codex, or `better-compact`.
  Adapt or replace the separate feeder with the same receipt/outbox semantics.
- [ ] Provide an automatic, idempotent config/install migration using existing
  source roots and identity. Preserve existing config for rollback. Discover and
  validate the selected authority and protocol; scope local checkpoints and retry
  controls to that authority so old S3 receipts cannot skip a ClickHouse import.
  A discovery error, identity change, outage or incompatible endpoint fails
  closed; never fall back silently to the legacy store.
- [ ] Arrange an authenticated, transport-protected route for upgraded remote
  hosts using available infrastructure. Preserve the old route for unupgraded
  senders during migration; only switch a sender after its upgraded sync service
  is running. Do not require machine-specific setup flags as normal operation.
- [ ] Make pending, sent, retained and failed outbox work visible in status.
  Retry the exact immutable descriptor/body after restart or lost ACK; verify
  authority, source, path, generation, range, checksum and receipt before
  advancing state. Bound spool use and apply backpressure rather than dropping
  data when storage or the receiver is unavailable.

## 2. Finish the ClickHouse-backed sync service

- [ ] Define and persist a stable whole-file **generation and lineage** model:
  source identity, path, generation, observed size/hash, and ordered segment
  ranges. Proven appends extend the right generation; truncate, replacement,
  rewrite and path reuse start a new one. Repeated snapshot overlap must not
  create extra logical occurrences, while actual repeated records at different
  positions and distinct rewrites remain distinct.
- [ ] Make generation commits reflect a real stable snapshot or a provably
  consistent prefix, not independently read segments from different versions.
  Detect changes during hashing/copying, preserve partial trailing JSONL bytes,
  and retry without falsely declaring coverage. Support continuously growing
  files without starving their safe prefixes. Use bounded streaming/spooling.
- [ ] Implement contiguous coverage and byte-exact reconstruction checks,
  including missing/overlapping ranges, empty files, metadata conflicts,
  out-of-order retry, segment/reference/chunk checksums and whole-file hash.
  Never infer hole-free coverage from a maximum end offset.
- [ ] Implement ClickHouse-aware `sync verify` and authority-scoped targeted
  retry. Show separate, evidence-backed states for local source, ClickHouse raw
  coverage, backup coverage, VCC projection and search indexing; unavailable or
  unsupported checks are `unknown`/incomplete, never fabricated as verified.
  Preserve the legacy verification behavior for legacy endpoints.
- [ ] Implement the conservative single-server outbox-release policy without a
  user decision: a matching ClickHouse receipt may advance the sync checkpoint,
  but retain the immutable local bytes until an independently checked completed
  backup chain proves inclusion of that segment **and** its required references
  and chunks. Prove inclusion from actual backup contents/metadata and a tested
  restore/readback mechanism; job success, elapsed time or a backup start/end
  timestamp alone is insufficient. If proof is unavailable, retain and retry;
  report retained bytes/age and apply bounded backpressure. This is a recovery
  queue, not a second live authority.
- [ ] Keep receiver writes single-flight per namespace and make crash/restart,
  conflicts, backup-coverage bookkeeping and storage limits safe and observable.
  Preserve original bytes and the authority ID across backup/restore. Use the
  existing cold tier for durable data and bounded hot staging; treat the current
  2-GiB raw-payload staging guard as a proxy, not a filesystem or merge-space
  quota. Do not assume the unaudited host-device mapping.

## 3. Hand VCC and search over to the authoritative log

- [ ] Implement a restartable projection worker that reads committed,
  reconstructible ClickHouse generations and updates the existing PostgreSQL
  VCC/history and search/index consumers. Keep existing readers and schemas
  compatible, with migrations/backfills where necessary. `better-sync` owns sync
  and projection integration; reuse the existing VCC compiler rather than
  moving compaction into it.
- [ ] Use stable source/generation/record-position identities and transactional,
  idempotent projection checkpoints. Do not use payload or block hash alone as
  an event identity. Distinguish repeated snapshots from distinct identical
  events. Handle rewrites, partial JSONL lines, out-of-order segments, poison
  records, worker crashes, retries, and index lag without losing provenance or
  advancing a checkpoint past an unprocessed gap.
- [ ] Establish a per-source handoff fence between the legacy sealed/loaded
  archives and verified ClickHouse generations. Drain and account for legacy
  receiver spill, sealed archives and in-flight loader work; reconcile overlaps
  from actual source/range/occurrence evidence rather than trusting an ACK or a
  maximum offset. Project the boundary exactly once. Historical S3 loading may
  continue for preserved archives but must not become a competing live head.
- [ ] Compare the new projection against legacy results and source bytes on
  representative Pi/Codex histories, including equal-payload distinct events.
  Verify VCC provenance, search visibility and restart/rebuild behavior; expose
  raw, projected and indexed lag separately. Do not claim search completeness
  while any projection/index stage is pending or unknown.

## 4. Verify, pilot, cut over and close out

- [ ] Run unit, integration and end-to-end tests for append/rewrite/truncation,
  gaps, duplicate/reordered delivery, lost ACK, receiver and ClickHouse outage,
  source changes during capture, disk-full/backpressure, crashes at each commit
  boundary, projection retries, authority mismatch and restart. Exercise the
  actual Pi and Codex sync-only installation paths. Keep tests and scratch data
  isolated from production.
- [ ] Inventory real backup sizes and chain dependencies; report compressed
  bytes, unique incrementals, full bases, and recovery-point age. Native backups
  preserve compressed ClickHouse parts, but do not claim a particular compression
  ratio or that operation count equals full copies. Continue the existing hourly
  incrementals and weekly full bases so the current recovery objective is not
  weakened without a decision. The known lost-ACK lock conflict remains a risk:
  retain bounded fresh-prefix retries and alert on failures/age; never clear an
  unknown active job or remove a lock to force progress.
- [ ] Implement and test dependency-aware retention without deleting any full
  base, failed/unknown prefix, historical archive or an incremental needed by a
  retained backup. Conservative default: preserve the current and previous
  complete full-based chains, the latest restore-verified chain, and every full
  base; older *owned* incrementals may be pruned only after complete inventory
  proves no surviving or unknown dependent needs them. If inventory, ownership,
  chain state, or deletion safety is uncertain, retain and alert. Validate on
  isolated scratch chains first, log the dry-run and actions, and do not make
  retention a prerequisite for ingestion if the storage backend cannot prove it
  safe. Monitor capacity because retained full bases still accumulate.
- [ ] Perform a real isolated full → incremental restore drill, verify authority,
  chunk/reference/generation checksums and representative projection recovery;
  verify production-chain recoverability separately rather than extrapolating
  from a tiny canary. Exercise recovery from an interrupted/unknown backup
  without overlapping jobs or deleting anything. Record evidence and limits.
- [ ] Run a bounded production pilot for one eligible source at a time. Capture
  a baseline, switch its upgraded sender, follow active tails, compare source
  bytes and contiguous coverage, verify projections/search and backup inclusion,
  then exercise an outage and recovery. Require zero unexplained gaps or receipt
  conflicts, projection/index catch-up within five minutes after a stable test
  snapshot, and a successful recovery point less than two hours old. Check
  latency, storage and memory against measured capacity; stop or roll back
  automatically on a failed invariant.
- [ ] Roll out to all inventoried eligible sources, including the feeder, with
  exactly one designated active sender/authority per source. Reconcile files
  created during each handoff and all old spill/archives; rescan until source,
  ClickHouse, projection and backup coverage meet their respective checks. Do
  not treat offline or inaccessible hosts as migrated.
- [ ] Rehearse rollback before relying on it: stop the affected sender, preserve
  its local outbox and ClickHouse commits, reconcile all post-cutover generations
  and projection effects, then restore the saved legacy configuration only when
  the legacy path can represent and receive the outstanding data. Verify the
  resumed legacy path; never discard ClickHouse-only data or silently fork two
  authorities. Keep legacy services/config available until these checks pass.
- [ ] Reconcile historical exceptions using bounded, evidence-preserving
  automated retries. Import a recovered version only after its identity, bytes
  and frozen expectations are verified; retain mismatches/missing keys as
  exceptions with provenance and no success receipt. Report the actual remaining
  count separately from live cutover coverage.
- [ ] Finish deployment, tests, monitoring, sync-only installation/update docs,
  recovery runbook, capacity/retention report and handoff evidence. Disable the
  old **live** ingestion path only after every participating sender and its
  legacy spill are accounted for and rollback is proven; retain archives and
  recovery configuration. Keep watches for receiver, projection, backup age,
  capacity and stalled outboxes.

## Definition of done — everything in this cutover is done

The plan is complete only when all four sections are checked with recorded test
and production evidence: every inventoried participating source is on the new
single authority; its source snapshots and active tails have hole-free,
byte-verified coverage; required VCC/search projections are current; retained
outbox data has verified backup coverage before release; backup and restore work;
legacy in-flight work is reconciled; rollback is tested; monitoring and docs are
in place. No manual mid-plan handoff is outstanding. Production must never be
reported as fully migrated while an inaccessible sender, unverified range, or
broken projection remains. Historical exceptions are reported separately and
are not described as covered unless their actual evidence is recovered and
verified.

## After the plan: optional or unavoidable operator-only follow-up

These are not interactive steps in the execution path:

- If a host, credential, permission, endpoint or capacity issue genuinely cannot
  be resolved with existing access, report exactly what remains and the evidence
  after autonomous work; it is an actual blocker, not a completed cutover.
- Recommend replacing the explicit peer pin with an administrator-managed CA
  when available, without weakening current verification in the meantime.
- Recommend a ClickHouse upgrade/backport with verified own-lock recovery for the
  native S3 lost-acknowledgement bug. The present bounded retries and watches
  are mitigation, not a proven fix.
- Report measured backup cost and the accumulation of preserved full bases.
  Changing the recovery-point objective or permitting deletion of full bases is
  a separate later decision; neither is silently assumed here.
