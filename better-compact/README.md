# better-compact

Bounded, validated **VCC recovery compaction** for OpenCode V1 and Pi. This
package carries the compaction and nothing else: no CLI, no services.

## Install

```sh
pi install npm:better-compact        # or: pi install ./better-compact
```

Pi registers both entry points from the package manifest (`src/pi.ts`,
`src/cat.ts`) and keeps them current with `pi update --extensions`. For
OpenCode, point a plugin tuple at this package's `runtime/` (or `dist/`)
directory; `scripts/configure.ts` manages that tuple for you.

## What is inside

| path | purpose |
|---|---|
| `src/pi.ts`, `src/cat.ts` | Pi extension entry points (`pi.extensions`) |
| `src/index.ts`, `src/server.ts`, `src/tui.ts` | OpenCode V1 plugin + compaction-model selector |
| `src/vcc*.ts`, `src/ledger.ts`, `src/semantic.ts`, ... | the VCC compaction machinery and `vcc_recall` |
| `runtime/` | bundled runtime entries for OpenCode |
| `scripts/configure.ts` | manages the OpenCode plugin tuple (model, entries) |
| `eval/` | compaction quality evaluation harness |

VCC modes and the exact-recall contract are documented in the repository
README. The session sync pipeline lives in the sibling **better-sync**
package; neither package depends on the other.
