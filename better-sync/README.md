# better-sync

Session **sync CLI and services**: S3 upload with reconcile, coverage verify,
the background sync daemon, the legacy Postgres mirror and the RAG worker.
This package carries the CLI and services and nothing else: no compaction.

## Install

```sh
npm install -g better-sync          # or: pi install npm:better-sync
better-sync sync setup --url <session-center-url> --token <bearer>
better-sync sync install            # start the background service
```

For HTTP receivers, setup checks authenticated capabilities and automatically
selects a compatible ClickHouse or legacy receiver. A legacy receiver must
explicitly return 404 for `/capabilities`; outages, authentication failures and
incompatible ClickHouse protocols fail setup instead of silently selecting a
different authority. Setup therefore requires the selected receiver to be
reachable. Existing configs are not automatically switched.

Sync-only hosts (no OpenCode compaction anywhere) can use the bundled
installer instead:

```sh
sh install-sync-only.sh -- --url <session-center-url> [--token <bearer>]
```

`install.sh` orchestrates the full host setup (both packages); it expects the
compaction package next to this one in a checkout.

## Commands

`better-sync --help` lists them: `sync setup|run|status|verify|install|uninstall|retry-s3|compact|migrate|reconcile|prune|backfill-hierarchy`,
`install [pi]`, `update`, `doctor`, `installation reset|adopt`, `rag setup`.

The compaction (`vcc_mode`, model selection) is configured by the sibling
**better-compact** package; neither package depends on the other.
