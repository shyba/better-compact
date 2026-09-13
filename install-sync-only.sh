#!/bin/sh
# Sync-only installer: installs the safe-compaction sync daemon WITHOUT
# requiring OpenCode. Use on hosts that only need to feed sessions into
# the corpus (no compaction hooks). The plugin proper (compaction inside
# OpenCode) still requires OpenCode — install that with install.sh.
#
# Usage:
#   curl -fsSL <raw>/install-sync-only.sh | sh -s -- --url <session-center URL> [--token <bearer>]
#   # or set OPENCODE_SAFE_COMPACTION_SYNC_URL / OPENCODE_SAFE_COMPACTION_TOKEN and run: sh install-sync-only.sh
#
# Env: OPENCODE_SAFE_COMPACTION_DIR (install dir, default ~/.local/share/better-compact)
#      OPENCODE_SAFE_COMPACTION_REF (branch/commit, default 'default')

set -u

say() { printf 'opencode-safe-compaction(sync-only): %s\n' "$1"; }
fail() { printf 'opencode-safe-compaction(sync-only): %s\n' "$1" >&2; exit 1; }

SYNC_ONLY_ARGS=""
for arg in "$@"; do
  case "$arg" in
    --sync-only) ;; # accepted silently for symmetry
    *) SYNC_ONLY_ARGS="$SYNC_ONLY_ARGS $arg" ;;
  esac
done

repository=${OPENCODE_SAFE_COMPACTION_REPO:-https://github.com/shyba/better-compact.git}
ref=${OPENCODE_SAFE_COMPACTION_REF:-default}
install_dir=${OPENCODE_SAFE_COMPACTION_DIR:-}
if [ -z "$install_dir" ]; then
  install_dir=${HOME:?HOME must be set}/.local/share/better-compact
  legacy_install_dir=${HOME}/.local/share/opencode/plugins/safe-compaction
  if [ ! -e "$install_dir" ] && [ -d "$legacy_install_dir/.git" ]; then install_dir=$legacy_install_dir; fi
fi

case "$install_dir" in /*) ;; *) fail "install directory must be absolute: $install_dir" ;; esac
[ "$install_dir" != "${HOME%/}" ] || fail "install directory is unsafe: $install_dir"

command -v git >/dev/null 2>&1 || fail "required command not found: git"
git_bin=$(command -v git)

# bun (same minimum policy as install.sh; bootstrap is inherited from it)
if command -v bun >/dev/null 2>&1; then
  bun_bin=$(command -v bun)
else
  say "bun not found; bootstrapping"
  curl -fsSL https://bun.sh/install | bash >/dev/null 2>&1 || fail "could not bootstrap bun"
  bun_bin="$HOME/.bun/bin/bun"
  [ -x "$bun_bin" ] || fail "bun bootstrap did not produce an executable"
fi

say "cloning $repository into $install_dir"
if [ -d "$install_dir/.git" ]; then
  say "$install_dir exists; pulling $ref"
  "$git_bin" -C "$install_dir" fetch --depth 1 origin "$ref" || fail "fetch failed"
  "$git_bin" -C "$install_dir" checkout --detach FETCH_HEAD || fail "checkout failed"
else
  mkdir -p "$(dirname "$install_dir")"
  "$git_bin" clone --branch "$ref" --depth 1 -- "$repository" "$install_dir" || fail "clone failed"
fi

cd "$install_dir" || fail "install dir vanished"
"$bun_bin" install --frozen-lockfile || fail "bun install failed"

# sync setup + systemd user wiring (no OpenCode anywhere)
# shellcheck disable=SC2086
"$bun_bin" scripts/cli.ts sync setup $SYNC_ONLY_ARGS || fail "sync setup failed (supply --url/--token or the sync env)"
"$bun_bin" scripts/cli.ts sync install || fail "sync install failed"
"$bun_bin" scripts/cli.ts sync status || say "sync status unavailable; check 'bun scripts/cli.ts sync status' later"

say "done: sync daemon installed (systemd --user). OpenCode compaction hooks were NOT installed (no OpenCode on this host)."
