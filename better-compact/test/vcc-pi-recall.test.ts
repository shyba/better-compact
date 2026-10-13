import { describe, expect, test } from "bun:test"
import { buildVccPiRecallIndex, discoverVccPiHandles, getVccPiRecallIndex, recallEntryExcerpt, recallEntrySnippet } from "../src/vcc-pi-recall.js"
import type { SessionEntry } from "@earendil-works/pi-coding-agent"
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent"
import {
  buildVccPiRecallIndex,
  discoverVccPiHandles,
  renderVccPiRecallEntry,
  resolveVccPiHandle,
  vccPiRecallToolResult,
} from "../src/vcc-pi-recall.js"
import piExtension from "../src/pi.js"

function message(id: string, text: string, parentId: string | null = null): SessionEntry {
  return {
    type: "message",
    id,
    parentId,
    timestamp: "2026-08-20T00:00:00.000Z",
    message: { role: "user", content: text, timestamp: 1 },
  } as unknown as SessionEntry
}

function compaction(id: string, summary: string): SessionEntry {
  return {
    type: "compaction",
    id,
    parentId: null,
    timestamp: "2026-08-20T00:00:01.000Z",
    summary,
    firstKeptEntryId: "keep",
    tokensBefore: 1,
  }
}

function hiddenCustomMessage(id: string): SessionEntry {
  return {
    type: "custom_message",
    id,
    parentId: null,
    timestamp: "2026-08-20T00:00:01.000Z",
    customType: "hidden-context",
    content: "hidden api_key: sk-should-not-be-recalled",
    display: false,
  }
}

describe("Pi V1 exact recall resolver", () => {
  test("uses stable entry IDs and active-branch lineage, excludes generated summaries, and redacts", () => {
    const branch = [message("entry-a", 'retain api_key: "sk-12345678901234567890"'), compaction("summary", "generated secret api_key: should never be archived"), hiddenCustomMessage("hidden"), message("entry-b", "follow-up decision", "entry-a")]
    const first = buildVccPiRecallIndex({ session_id: "pi-session", branch })
    const second = buildVccPiRecallIndex({ session_id: "pi-session", branch: [...branch] })
    expect(first).toEqual(second)
    expect(first.complete).toBe(true)
    expect(first.entries.map((entry) => entry.entry_id)).toEqual(["entry-a", "entry-b"])
    expect(first.entries[0]!.handle).toMatch(/^archive:v1:pi:[a-f0-9]{64}:[a-f0-9]{64}:[a-f0-9]{64}$/)
    expect(first.entries[0]!.payload_bytes).not.toContain("sk-12345678901234567890")
    expect(renderVccPiRecallEntry(first.entries[0]!, 16_384)).toContain('"presentation":"transformed_redacted"')
  })

  test("verifies exact scope and payload, rejecting stale or tampered handles", () => {
    const index = buildVccPiRecallIndex({ session_id: "pi-session", branch: [message("entry-a", "decision")] })
    const entry = index.entries[0]!
    expect(resolveVccPiHandle({ handle: entry.handle, session_id: "pi-session", lineage_id: index.lineage_id, entries: index.entries })).toMatchObject({ ok: true })
    expect(resolveVccPiHandle({ handle: entry.handle, session_id: "other-session", lineage_id: index.lineage_id, entries: index.entries })).toMatchObject({ ok: false, reason: "scope_mismatch" })
    const tamperedHandle = `${entry.handle.slice(0, -1)}${entry.handle.endsWith("0") ? "1" : "0"}`
    expect(resolveVccPiHandle({ handle: tamperedHandle, session_id: "pi-session", lineage_id: index.lineage_id, entries: index.entries })).toMatchObject({ ok: false, reason: "handle_unavailable" })
    expect(resolveVccPiHandle({ handle: entry.handle, session_id: "pi-session", lineage_id: index.lineage_id, entries: [{ ...entry, payload_bytes: `${entry.payload_bytes}tampered` }] })).toMatchObject({ ok: false, reason: "digest_mismatch" })
  })

  test("preserves handles across append while isolating sibling branches", () => {
    const root = message("root", "shared root")
    const child = message("child", "active decision", "root")
    const initial = buildVccPiRecallIndex({ session_id: "pi-session", branch: [root, child] })
    const appended = buildVccPiRecallIndex({ session_id: "pi-session", branch: [root, child, message("tail", "later note", "child")] })
    const sibling = buildVccPiRecallIndex({ session_id: "pi-session", branch: [root, message("sibling", "other branch", "root")] })
    expect(appended.lineage_id).toBe(initial.lineage_id)
    expect(sibling.lineage_id).not.toBe(initial.lineage_id)
    expect(resolveVccPiHandle({ handle: initial.entries[0]!.handle, session_id: "pi-session", lineage_id: appended.lineage_id, entries: appended.entries })).toMatchObject({ ok: true })
  })

  test("re-resolves a handle after a restart-equivalent branch reload", () => {
    const branch = [message("entry-a", "restart-safe decision"), message("entry-b", "later note", "entry-a")]
    const first = buildVccPiRecallIndex({ session_id: "pi-session", branch })
    const restarted = buildVccPiRecallIndex({ session_id: "pi-session", branch: structuredClone(branch) })
    expect(restarted.lineage_id).toBe(first.lineage_id)
    expect(resolveVccPiHandle({
      handle: first.entries[0]!.handle,
      session_id: "pi-session",
      lineage_id: restarted.lineage_id,
      entries: restarted.entries,
    })).toMatchObject({ ok: true })
  })

  test("discovers deterministically, pages, bounds rendering, and keeps metadata payload-free", () => {
    const index = buildVccPiRecallIndex({ session_id: "pi-session", branch: [message("entry-a", "deploy decision alpha"), message("entry-b", "deploy decision beta"), message("entry-c", "unrelated")] })
    const page = discoverVccPiHandles({ query: "deploy decision", entries: index.entries, page: 2, max_results: 1 })
    expect(page).toMatchObject({ total: 2, page: 2, total_pages: 2 })
    expect(page.entries[0]!.entry_id).toBe("entry-b")
    expect(renderVccPiRecallEntry(page.entries[0]!, 256)).toBeUndefined()
    const result = vccPiRecallToolResult("ok", { operation: "handle", item: { payload: "secret" } }, 256)
    expect(result.output.length).toBeLessThanOrEqual(256)
    expect(result.metadata).toEqual({ status: "ok" })
    expect(result.metadata).not.toHaveProperty("item")
  })

  test("serves recall from the recent window of an over-capacity branch instead of refusing", async () => {
    // Regression: a branch longer than the index window used to answer every
    // call with status "incomplete"/reason "page_limit" and no items, so recall
    // was unusable in exactly the long sessions it exists for.
    const filler = Array.from({ length: 4_100 }, (_, index) => message(`filler-${index}`, `filler entry ${index}`))
    const branch = [...filler, message("recent-deploy", "the deploy decision was to roll back"), message("recent-note", "follow-up note", "recent-deploy")]
    const index = buildVccPiRecallIndex({ session_id: "pi-session", branch })
    expect(index.truncated).toBe(true)
    expect(index.branch_entries).toBe(4_102)
    expect(index.indexed_entries).toBeLessThanOrEqual(4_096)
    // the newest entries are the ones kept
    expect(index.entries.some((entry) => entry.entry_id === "recent-deploy")).toBe(true)
    expect(index.entries.some((entry) => entry.entry_id === "filler-0")).toBe(false)
    const page = discoverVccPiHandles({ query: "deploy decision", entries: index.entries })
    // the full match ranks first; the note is reachable through its parent link
    expect(page.entries.map((entry) => entry.entry_id)).toEqual(["recent-deploy", "recent-note"])
  })

  test("reports coverage in the tool response for a truncated branch", async () => {
    const filler = Array.from({ length: 4_100 }, (_, index) => message(`filler-${index}`, `filler ${index}`))
    const branch = [...filler, message("recent-deploy", "deploy decision recorded")]
    const tools = new Map<string, { execute: (...args: unknown[]) => Promise<{ content: Array<{ text: string }> }> }>()
    const api = { on() {}, registerCommand() {}, registerTool(tool: { name: string; execute: (...args: unknown[]) => Promise<{ content: Array<{ text: string }> }> }) { tools.set(tool.name, tool) } } as unknown as ExtensionAPI
    piExtension(api)
    const context = { sessionManager: { getSessionId: () => "pi-session", getBranch: () => branch } }
    const result = await tools.get("vcc_recall")!.execute("call-1", { query: "deploy decision" }, undefined, undefined, context)
    const body = JSON.parse(result.content[0]!.text) as { status: string; truncated: boolean; branch_entries: number; indexed_entries: number; total: number; items: Array<{ entry_id: string }> }
    expect(body.status).toBe("ok")
    expect(body.truncated).toBe(true)
    expect(body.branch_entries).toBe(4_101)
    expect(body.total).toBe(1)
    expect(body.items[0]!.entry_id).toBe("recent-deploy")
  })

  test("ranks partial query-term coverage instead of requiring every term", () => {
    const index = buildVccPiRecallIndex({ session_id: "pi-session", branch: [message("entry-a", "deploy decision alpha"), message("entry-b", "deploy beta"), message("entry-c", "unrelated")] })
    const page = discoverVccPiHandles({ query: "deploy decision", entries: index.entries })
    // entry-b matches only one term and is still reachable, after the full match
    expect(page.entries.map((entry) => entry.entry_id)).toEqual(["entry-a", "entry-b"])
    expect(page.total).toBe(2)
  })

  test("keeps recall usable when some entries have an unusable identity", async () => {
    // Regression: a single recall-eligible entry with an unusable id (numeric,
    // or containing a separator) used to mark the whole session incomplete, so
    // ordinary sessions answered every call with an error and no items.
    const branch = [message("entry-valid", "the deploy decision was to roll back"), message("12345", "numeric id entry"), { ...message("bad:id", "separator id entry") }]
    const index = buildVccPiRecallIndex({ session_id: "pi-session", branch })
    expect(index.complete).toBe(false)
    expect(index.reason).toBe("unsupported_record")
    expect(index.entries.map((entry) => entry.entry_id)).toEqual(["entry-valid"])
    expect(index.skipped_entries).toBe(2)

    const tools = new Map<string, { execute: (...args: unknown[]) => Promise<{ content: Array<{ text: string }> }> }>()
    const api = { on() {}, registerCommand() {}, registerTool(tool: { name: string; execute: (...args: unknown[]) => Promise<{ content: Array<{ text: string }> }> }) { tools.set(tool.name, tool) } } as unknown as ExtensionAPI
    piExtension(api)
    const context = { sessionManager: { getSessionId: () => "pi-session", getBranch: () => branch } }
    const result = await tools.get("vcc_recall")!.execute("call-1", { query: "deploy decision" }, undefined, undefined, context)
    const body = JSON.parse(result.content[0]!.text) as { status: string; total: number; items: Array<{ entry_id: string }>; skipped_entries: number }
    expect(body.status).toBe("ok")
    expect(body.total).toBe(1)
    expect(body.items[0]!.entry_id).toBe("entry-valid")
    expect(body.skipped_entries).toBe(2)
  })

  test("reports duplicate active-branch IDs as incomplete", () => {
    const index = buildVccPiRecallIndex({ session_id: "pi-session", branch: [message("entry-a", "one"), message("entry-a", "two")] })
    expect(index).toMatchObject({ complete: false, reason: "duplicate_message_id" })
  })

  test("reports malformed authoritative entries as incomplete", () => {
    const invalid = { ...message("bad:id", "invalid") }
    const index = buildVccPiRecallIndex({ session_id: "pi-session", branch: [invalid] })
    expect(index).toMatchObject({ complete: false, reason: "unsupported_record" })
  })

  test("registers the public tool and reads only the active branch", async () => {
    const branch = [message("entry-a", 'retain api_key: "sk-12345678901234567890"')]
    const tools = new Map<string, { execute: (...args: unknown[]) => Promise<{ content: Array<{ text: string }>; details: { status: string } }> }>()
    const api = {
      on() {},
      registerCommand() {},
      registerTool(tool: { name: string; execute: (...args: unknown[]) => Promise<{ content: Array<{ text: string }>; details: { status: string } }> }) {
        tools.set(tool.name, tool)
      },
    } as unknown as ExtensionAPI
    piExtension(api)
    const tool = tools.get("vcc_recall")
    expect(tool).toBeDefined()
    const context = { sessionManager: { getSessionId: () => "pi-session", getBranch: () => branch } }
    const discovered = await tool!.execute("call-1", { query: "retain api_key", max_bytes: 4_096 }, undefined, undefined, context)
    const discoveredBody = JSON.parse(discovered.content[0]!.text) as { status: string; items: Array<{ handle: string }> }
    expect(discoveredBody.status).toBe("ok")
    expect(discoveredBody.items).toHaveLength(1)
    const expanded = await tool!.execute("call-2", { handle: discoveredBody.items[0]!.handle, max_bytes: 4_096 }, undefined, undefined, context)
    expect(expanded.details).toEqual({ status: "ok" })
    expect(expanded.content[0]!.text).not.toContain("sk-12345678901234567890")
    const tiny = await tool!.execute("call-4", { query: "retain api_key", max_bytes: 256 }, undefined, undefined, context)
    expect(new TextEncoder().encode(tiny.content[0]!.text).byteLength).toBeLessThanOrEqual(256)
    expect(JSON.parse(tiny.content[0]!.text)).toMatchObject({ status: "oversized" })
    const unavailable = await tool!.execute("call-3", { scope: "all", query: "retain" }, undefined, undefined, context)
    expect(JSON.parse(unavailable.content[0]!.text)).toMatchObject({ status: "unavailable", reason: "scope_unavailable" })
  })
})

describe("Pi V1 recall performance and hygiene", () => {
  const entry = (id: string, text: string) => ({
    id, parentId: null, timestamp: new Date(1760000000000).toISOString(),
    type: "message" as const,
    message: { role: "user" as const, parts: [{ type: "text" as const, text }] },
  })

  test("indexes identical output incrementally and excludes recall traffic by default", () => {
    const base = [entry("entry-a", "notes about the migration"), entry("entry-b", "single connection pool")]
    const full = buildVccPiRecallIndex({ session_id: "pi-perf", branch: base })
    const grown = [...base, entry("entry-c", "more migration work")]
    const incremental = getVccPiRecallIndex({ session_id: "pi-perf", branch: grown })
    const rebuilt = buildVccPiRecallIndex({ session_id: "pi-perf", branch: grown })
    expect(incremental.entries.map((item) => item.entry_id)).toEqual(rebuilt.entries.map((item) => item.entry_id))
    expect(incremental.lineage_id).toBe(rebuilt.lineage_id)
    expect(incremental.complete).toBe(rebuilt.complete)

    const traffic = entry("entry-t", JSON.stringify({ kind: "vcc_recall", status: "ok", note: "migration" }))
    const index = buildVccPiRecallIndex({ session_id: "pi-perf-2", branch: [entry("entry-x", "migration note"), traffic] })
    const hidden = discoverVccPiHandles({ query: "migration", entries: index.entries, max_results: 8 })
    expect(hidden.total).toBe(1)
    const shown = discoverVccPiHandles({ query: "migration", entries: index.entries, max_results: 8, include_recall_traffic: true })
    expect(shown.total).toBe(2)
  })

  test("discovery results carry readable snippets", () => {
    const index = buildVccPiRecallIndex({ session_id: "pi-perf-3", branch: [entry("entry-a", `${"x".repeat(400)} the pardal adapter identity bug ${"y".repeat(400)}`)] })
    const snippet = recallEntrySnippet(index.entries[0]!, ["pardal", "adapter"])
    expect(snippet).toContain("pardal adapter identity bug")
    expect(snippet.length).toBeLessThan(400)
  })
})

describe("Pi V1 recall policy, budgets and coverage", () => {
  const entry = (id: string, message: unknown) => ({
    id, parentId: null, timestamp: new Date(1760000000000).toISOString(),
    type: "message" as const,
    message,
  })

  test("excludes reasoning and provider bookkeeping by default and restores them on request", () => {
    const message = {
      role: "assistant",
      content: [
        { type: "text", text: "visible answer about the migration" },
        { type: "thinking", thinking: "internal chain of thought 9f3a" },
      ],
      usage: { total_tokens: 4242 },
    }
    const redacted = buildVccPiRecallIndex({ session_id: "pi-policy", branch: [entry("entry-a", message)] })
    expect(redacted.entries[0]!.payload_bytes).toContain("visible answer")
    expect(redacted.entries[0]!.payload_bytes).not.toContain("internal chain of thought 9f3a")
    expect(redacted.entries[0]!.payload_bytes).not.toContain("4242")

    const full = buildVccPiRecallIndex({ session_id: "pi-policy", branch: [entry("entry-a", message)], include_metadata: true })
    expect(full.entries[0]!.payload_bytes).toContain("internal chain of thought 9f3a")
    expect(full.entries[0]!.payload_bytes).toContain("4242")
  })

  test("oversized retrieval degrades to an excerpt with the required budget", () => {
    const big = entry("entry-big", { role: "user", content: [{ type: "text", text: Array.from({ length: 2_000 }, (_, i) => `needle in haystack line ${i}`).join(" ") }] })
    const index = buildVccPiRecallIndex({ session_id: "pi-budget", branch: [big] })
    const excerpt = recallEntryExcerpt(index.entries[0]!, 512)
    expect(excerpt.presentation).toBe("excerpt_redacted")
    expect(excerpt.excerpt.length).toBeGreaterThan(0)
    expect(excerpt.excerpt.length).toBeLessThanOrEqual(512)
    expect(excerpt.required_bytes).toBeGreaterThan(512)
    expect(excerpt.hint).toContain(String(excerpt.required_bytes))
  })

  test("result pagination reports truncation separately from window coverage", () => {
    const branch = [entry("entry-a", { role: "user", content: [{ type: "text", text: "shared phrase one" }] }),
      entry("entry-b", { role: "user", content: [{ type: "text", text: "shared phrase two" }] })]
    const index = buildVccPiRecallIndex({ session_id: "pi-coverage", branch })
    const first = discoverVccPiHandles({ query: "shared phrase", entries: index.entries, page: 1, max_results: 1 })
    expect(first).toMatchObject({ total: 2, total_pages: 2, results_truncated: true })
    const last = discoverVccPiHandles({ query: "shared phrase", entries: index.entries, page: 2, max_results: 1 })
    expect(last).toMatchObject({ total: 2, page: 2, results_truncated: false })
  })

  test("cached recall is a regression guard against per-call re-indexing", () => {
    const branch = Array.from({ length: 2_000 }, (_, i) => entry(`entry-${i}`, { role: "user", content: [{ type: "text", text: `note ${i} ${"x".repeat(400)}` }] }))
    const started = performance.now()
    getVccPiRecallIndex({ session_id: "pi-perf-guard", branch })
    const cold = performance.now() - started
    const again = performance.now()
    getVccPiRecallIndex({ session_id: "pi-perf-guard", branch })
    const warm = performance.now() - again
    // A warm call must never pay the index build again; the margin is wide
    // enough to ignore scheduling noise while catching a full rebuild.
    expect(warm).toBeLessThan(Math.max(50, cold / 2))
  })
})
