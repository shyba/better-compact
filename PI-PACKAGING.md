# Packaging the Pi extensions as a proper Pi package

Status: **investigated, not implemented** (parked). This document records what was
verified, what blocks a plain `pi install npm:...`, and the recommended change
list. No code changes accompany this file.

Measured on the 2026-09-13 checkout of `better-compact` (`default`), with Pi
`@earendil-works/pi-coding-agent` as installed on the dev host.

## What already works (verified, not assumed)

1. **`pi install git:github.com/shyba/better-compact` works today.**

   ```sh
   PI_CODING_AGENT_DIR=/tmp/sbx pi install git:github.com/shyba/better-compact
   ```

   Clones to `$PI_CODING_AGENT_DIR/git/github.com/shyba/better-compact`, records
   the source in `settings.json`, and resolves `package.json` → `pi.extensions`.
   Loading it through the SDK (`DefaultResourceLoader` + `createAgentSession`):

   - 2 extensions loaded (`src/pi.ts`, `src/cat.ts`), **0 load errors**
   - `vcc_recall` registered **exactly once**, no duplicate tool names
   - commands registered: `/compaction-model` (pi.ts), `/cat` (cat.ts)

2. **A dist-only package (the shape npm would ship) also loads.**

   ```sh
   bun run build:js
   # package.json: { "pi": { "extensions": ["./dist/pi.js", "./dist/cat.js"] } }
   pi install /tmp/pipkg   # → 2 extensions, 0 errors, vcc_recall once
   ```

   Bundle shape: `dist/pi.js` 125 KB, `dist/cat.js` 22.6 KB; the only external
   imports are the host peers `@earendil-works/pi-ai` and
   `@earendil-works/pi-coding-agent`, plus Node builtins. The bundle is
   self-contained — no `src/`, no `node_modules` required.

3. **Load-time cost is small and service-free.** None of the extension graph
   (`pi.ts`, `pi-adapter.ts`, `semantic.ts`, `ledger.ts`, `validation.ts`,
   `projection.ts`, `cat-core.ts`, `vcc-pi-recall.ts`) imports Postgres,
   Qdrant or SQLite at module scope. The extension loads with no configuration
   and no running services.

## What blocks the usual path (`pi install npm:...`)

1. **npm name collision — the important finding.**

   | candidate | npm status |
   | --- | --- |
   | `better-compact` | **taken** — `better-compact@0.2.6`, unrelated AGPL project (`AshishKumar4/better-compact`, an OpenCode plugin); has no `pi` key, so it installs and does nothing |
   | `pi-better-compact` | taken (`1.0.1`) |
   | `@shyba/better-compact` | free |
   | `opencode-safe-compaction` | free (current `package.json` name; not published) |
   | `better-compact-pi` | free |

   Installing `npm:better-compact` today pulls a stranger's package. Publishing
   must use a different name — `@shyba/better-compact` is the natural choice.

2. **`package.json` is not publishable as-is**: `private: true`, name
   `opencode-safe-compaction` (OpenCode-centric), no `repository`, no `author`.

3. **Manifest points at TypeScript sources** (`./src/pi.ts`, `./src/cat.ts`).
   Verified working for git and local installs. For npm, either ship `src/` in
   the tarball (keeps one manifest for both) or point the manifest at `dist/`
   (smaller, no TS loading); `dist/` is deliberately untracked, so a dist
   manifest needs `prepack` (already defined) to build it at publish time.

## Behaviour gaps for a *simple* VCC-compaction extension

These are independent of packaging, and they are what a first-time user hits:

1. **Config is project-scoped only.** `loadPiOptions` reads
   `<cwd>/.pi/safe-compaction.json` (`src/pi-adapter.ts`); there is no global
   file. A fresh install in a new project silently runs on defaults.
   `getAgentDir()` is exported by `@earendil-works/pi-coding-agent`, so a global
   `$PI_CODING_AGENT_DIR/safe-compaction.json` with project override is cheap to
   add.
2. **Default `vcc_mode` is `"off"`** (`src/options.ts`). Install the "VCC
   compaction" extension and, until configured, it performs no VCC compaction.
3. **Two concerns in one package**: pi.ts (VCC compaction, `vcc_recall`,
   `/compaction-model`) and cat.ts (pinned files, `/cat`). If "simple" means one
   job per extension, the manifest can list just `./dist/pi.js`.
4. **Identity in Pi's extension list is a file path**, not a friendly name.

## Recommended change list (when unparked)

1. Choose the published name (`@shyba/better-compact` recommended) and make the
   package publishable: `private: false`, add `repository`, `author`,
   `homepage`, `files: ["dist", "src", "package.json"]`; keep `pi.extensions`
   source-first so a single manifest serves git and npm.
2. Add global-then-project config resolution (`getAgentDir()` first,
   `<cwd>/.pi/safe-compaction.json` overrides) with tests, and document the
   two-line global setup.
3. Decide the pi default for `vcc_mode` (`hybrid` for zero-config VCC, or keep
   `off` plus a first-run notice / install-time hint) and document it in the
   README install section.
4. README: an "install from the usual path" section —
   `pi install git:github.com/shyba/better-compact` (works now) and
   `pi install npm:@shyba/better-compact` (after publish) — replacing the
   `install.sh` shim/`pi install <install-dir>` guidance for Pi users.
5. Optional: split a Pi-only package if the reviewable surface should be just
   the VCC extension.

## Reproduce

```sh
# git install + loader audit
PI_CODING_AGENT_DIR=/tmp/sbx pi install git:github.com/shyba/better-compact
PI_CODING_AGENT_DIR=/tmp/sbx pi list

# dist-only (npm shape) install
bun run build:js
ls -la dist/pi.js dist/cat.js

# name availability
npm view better-compact version      # → unrelated project
npm view @shyba/better-compact version  # → 404 (free)
```
