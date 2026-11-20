#!/usr/bin/env bash
# Fleet rollout: install the ClickHouse-backed sync sender on one remote host.
# Usage: fleet-rollout.sh [user@]host [remote-user-home]
# Requires: ssh access to the host, rsync, and the receiver reachable from it
# at http://192.168.0.31:4176 (bearer token; tunnel if the host prefers).
set -euo pipefail
HOST="${1:?usage: fleet-rollout.sh [user@]host [remote-home]}"
RHOME="${2:-}"
REPO="/secondary/data/session_center/opencode-better-compact-plugin/better-sync"
TOKEN="$(grep S3_SYNC_TOKEN /pg-hot/data/better-sync/pi-pilot/home/sync.env | cut -d= -f2 | tr -d '"')"
RSTATE="$RHOME/.local/state/better-sync-fleet"

echo "== $HOST: connectivity"
ssh -o BatchMode=yes -o ConnectTimeout=8 "$HOST" 'echo ok' >/dev/null

echo "== $HOST: bun runtime"
ssh "$HOST" 'command -v bun >/dev/null || (curl -fsSL https://bun.sh/install | bash) ; export PATH="$HOME/.bun/bin:$PATH"; bun --version'

echo "== $HOST: linger for the user systemd instance"
ssh "$HOST" 'sudo loginctl enable-linger "$USER" 2>/dev/null || loginctl enable-linger 2>/dev/null || true'

echo "== $HOST: sync code"
ssh "$HOST" 'mkdir -p ~/.local/share/better-sync-fleet'
rsync -a --delete --exclude node_modules --exclude .git --exclude dist "$REPO/" "$HOST:.local/share/better-sync-fleet/better-sync/"
ssh "$HOST" 'export PATH="$HOME/.bun/bin:$PATH"; cd ~/.local/share/better-sync-fleet/better-sync && bun build --target=bun scripts/cli.ts --outdir dist >/dev/null && echo build_ok'

echo "== $HOST: authority discovery + pin (fail-closed)"
ssh "$HOST" "export PATH=\"\$HOME/.bun/bin:\$PATH\"
  mkdir -p ~/.config/better-sync-fleet ~/.local/state/better-sync-fleet/home
  printf '%s\n' 'SESSION_CENTER_URL=\"http://192.168.0.31:4176\"' 'S3_SYNC_TOKEN=\"$TOKEN\"' > ~/.local/state/better-sync-fleet/home/sync.env
  chmod 700 ~/.local/state/better-sync-fleet/home; chmod 600 ~/.local/state/better-sync-fleet/home/sync.env
  cd ~/.local/share/better-sync-fleet/better-sync
  BETTER_COMPACT_CONFIG=~/.config/better-sync-fleet/config.json \
  BETTER_COMPACT_STATE=~/.local/state/better-sync-fleet/state.sqlite \
  BETTER_COMPACT_HOME=~/.local/state/better-sync-fleet/home \
  bun dist/cli.js sync setup --url http://192.168.0.31:4176 --token '$TOKEN' --allow-insecure-remote"

echo "== $HOST: systemd user service"
ssh "$HOST" 'mkdir -p ~/.config/systemd/user'
ssh "$HOST" 'cat > ~/.config/systemd/user/better-sync-fleet.service <<UNIT
[Unit]
Description=better-sync ClickHouse sender (fleet)
After=network-online.target

[Service]
Type=simple
Environment=BETTER_COMPACT_CONFIG=%h/.config/better-sync-fleet/config.json
Environment=BETTER_COMPACT_STATE=%h/.local/state/better-sync-fleet/state.sqlite
Environment=BETTER_COMPACT_HOME=%h/.local/state/better-sync-fleet/home
EnvironmentFile=-%h/.local/state/better-sync-fleet/home/sync.env
ExecStart=%h/.bun/bin/bun %h/.local/share/better-sync-fleet/better-sync/dist/cli.js sync run
Restart=on-failure
RestartSec=15

[Install]
WantedBy=default.target
UNIT
systemctl --user daemon-reload && systemctl --user enable --now better-sync-fleet.service'

echo "== $HOST: verify"
sleep 12
ssh "$HOST" 'systemctl --user is-active better-sync-fleet.service' 
ssh "$HOST" "curl -sS -m 5 -H \"Authorization: Bearer \$(grep S3_SYNC_TOKEN ~/.local/state/better-sync-fleet/home/sync.env | cut -d= -f2 | tr -d '\"')\" http://192.168.0.31:4176/health"
echo
echo "== $HOST done: sender installed; discovered sources listed in ~/.config/better-sync-fleet/config.json"
