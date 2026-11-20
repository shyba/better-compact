# Fleet rollout — ClickHouse sync sender on producer hosts

The server side is complete: the committed-segment receiver listens on
`0.0.0.0:4176` (bearer token required, same posture as the legacy `:4175`
receiver), authority `299eaa56-36bf-4a40-9cfb-0d96445225bf` is pinned, and
the projector + watches handle production VCC. What remains per host is the
client sender.

## One command per host

From this host (ubuntu1 = 192.168.0.31):

    opencode-better-compact-plugin/better-sync/scripts/clickhouse/fleet-rollout.sh [user@]host [remote-home]

The script is idempotent. It: checks SSH, installs bun, rsyncs the sync code,
builds, runs `sync setup` (fail-closed authority discovery and pin against
`http://192.168.0.31:4176`), installs the `better-sync-fleet.service` user
unit, enables it, and verifies the daemon is active and the receiver is
reachable with the bearer token.

Known producer hosts (LAN neighbors, SSH open, no shared credentials yet):
`192.168.0.107` (has session history evidence: `--home-user-107-writer--`),
`.106`, `.109`, `.92`, `.81`. `workstation` is a logical name from the PG
inventory (drained Oct 2 — its data is already in ClickHouse).

## One-time recovery for hosts on the pre-restructure layout

Hosts whose checkout predates the two-package restructure (tree had `src/`
and `scripts/` at the repo root, e.g. commit `727c414c`) crash during
`better-compact update`: the fast-forward moves the code into `better-sync/`
and `better-compact/`, the old installer then cannot find
`scripts/configure.ts`, and the checkout rolls back. Recovery (once per
host, from the host itself):

    cd ~/.local/share/opencode/plugins/safe-compaction   # or the rolled-back checkout path
    git fetch origin && git checkout origin/default
    ./better-sync/install.sh

After this the new layout-aware installer owns the host; subsequent
`better-compact update` runs normally, the daemon auto-migrates to the
ClickHouse store per the migration semantics in
`CLICKHOUSE-CUTOVER-PLAN.md`, and `sync verify` shows coverage.

## What the operator must provide
- SSH access from this host to each producer host (add the pubkey in
  `~/.ssh/id_ed25519.pub` to each host's `authorized_keys`, or run the script
  from a host that has access).
- A decision on transport: the receiver speaks HTTP + bearer token on the LAN
  (identical to the legacy receiver). If any host requires encryption, tunnel
  `192.168.0.31:4176` over SSH and point that host's `SESSION_CENTER_URL` at
  the tunnel.

## After each host rolls out
1. `sync verify` on the host shows `local=verified clickhouse_raw=verified`.
2. The projector here picks the new source up automatically — add its source
   id to `check-pi-projection.py` SOURCES and to the projector runner args so
   production VCC advances for it (one line each).
3. Once a host's sources are current, retire any legacy feeder on it (same
   pattern: stop + disable, keep the unit for rollback).
