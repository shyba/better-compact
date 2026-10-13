import { describe, expect, test } from "bun:test"
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
