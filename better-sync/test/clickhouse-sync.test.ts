import { test, expect } from "bun:test"
import { createHash } from "node:crypto"
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { authorityStatePath, clickHouseOutboxStats, discoverAuthority, drainClickHouseOutbox, outboxDirectory, pinClickHouseAuthority, uploadClickHouseFile } from "../src/clickhouse-sync.js"

const authority = { store: "clickhouse" as const, protocol: 1 as const, authority_id: "test-authority-000001", max_segment_bytes: 4 }

test("authority-specific cursors never reuse legacy sent receipts", () => {
  expect(authorityStatePath("state", authority)).not.toBe("state")
  expect(authorityStatePath("state")).toBe("state")
})

test("authority pin survives restart and rejects a different identity", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "ch-pin-"))
  const state = path.join(root, "state.sqlite")
  try {
    await expect(pinClickHouseAuthority(state, authority, false)).rejects.toThrow("pin missing")
    await pinClickHouseAuthority(state, authority, true)
    await pinClickHouseAuthority(state, authority, false)
    await expect(pinClickHouseAuthority(state, { ...authority, authority_id: "different-authority-001" }, true)).rejects.toThrow("authority changed")
    await writeFile(`${state}.clickhouse-authority.json`, "invalid")
    await expect(pinClickHouseAuthority(state, authority, true)).rejects.toThrow("pin missing or invalid")
  } finally { await rm(root, { recursive: true, force: true }) }
})

test("pre-pin namespaced state cannot silently migrate to a different authority", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "ch-pin-upgrade-"))
  const state = path.join(root, "state.sqlite")
  try {
    await writeFile(authorityStatePath(state, authority), "prior state")
    await expect(pinClickHouseAuthority(state, { ...authority, authority_id: "different-authority-001" }, true)).rejects.toThrow("different ClickHouse authority state")
    await pinClickHouseAuthority(state, authority, true)
    await pinClickHouseAuthority(state, authority, false)
  } finally { await rm(root, { recursive: true, force: true }) }
})

test("legacy requires explicit 404; store outage fails closed", async () => {
  const server = Bun.serve({ port: 0, fetch: () => new Response("", { status: 404 }) })
  try { expect(await discoverAuthority(server.url.toString(), "token")).toBeUndefined() } finally { server.stop(true) }
  const failed = Bun.serve({ port: 0, fetch: () => new Response("", { status: 503 }) })
  try { await expect(discoverAuthority(failed.url.toString(), "token")).rejects.toThrow("503") } finally { failed.stop(true) }
})

test("durable pending segments survive failure and replay without source bytes", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "ch-outbox-"))
  const previous = process.env.BETTER_SYNC_OUTBOX
  process.env.BETTER_SYNC_OUTBOX = root
  let fail = true
  const bodies: string[] = []
  const server = Bun.serve({ port: 0, async fetch(request) {
    const data = await request.text()
    bodies.push(data)
    if (fail) return new Response("", { status: 503 })
    return Response.json({ authority_id: authority.authority_id, store: "clickhouse", committed: true,
      sha256: request.headers.get("x-vcc-sha256"), source_id: request.headers.get("x-vcc-source-id"), source_kind: request.headers.get("x-vcc-source-kind"), locator: request.headers.get("x-vcc-source-locator"),
      file_id: decodeURIComponent(new URL(request.url).pathname.slice("/file/".length)), path: request.headers.get("x-vcc-path"),
      start: Number(request.headers.get("x-vcc-start")), size: Buffer.byteLength(data), full: request.headers.get("x-vcc-full") === "true" })
  } })
  try {
    const filename = path.join(root, "source.jsonl")
    await writeFile(filename, "payload")
    const input = { endpoint: server.url.toString(), token: "token", fileID: "file", sourcePath: "source.jsonl", filename, size: 7, start: 0, full: true, source: { id: "a".repeat(32), kind: "pi-jsonl", locator: root } }
    await expect(uploadClickHouseFile(input, authority)).rejects.toThrow("outbox retained")
    expect((await readdir(outboxDirectory(authority))).some(name => name.endsWith(".json"))).toBe(true)
    await rm(filename)
    fail = false
    await drainClickHouseOutbox(input, authority)
    expect(bodies).toEqual(["payl", "payl"])
    const names = await readdir(outboxDirectory(authority))
    expect(names.filter(name => name.endsWith(".json"))).toHaveLength(1)
    expect(names.filter(name => name.endsWith(".body"))).toHaveLength(1)
    expect(await clickHouseOutboxStats(authority)).toMatchObject({ pending: 0, retained: 1 })
    await drainClickHouseOutbox(input, authority)
    expect(bodies).toEqual(["payl", "payl"]) // committed copy is not resent or removed
  } finally {
    server.stop(true)
    if (previous === undefined) delete process.env.BETTER_SYNC_OUTBOX
    else process.env.BETTER_SYNC_OUTBOX = previous
    await rm(root, { recursive: true, force: true })
  }
})

test("orphaned outbox bodies are never overwritten and identical bodies can resume", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "ch-orphan-"))
  const previous = process.env.BETTER_SYNC_OUTBOX
  process.env.BETTER_SYNC_OUTBOX = root
  let requests = 0
  const server = Bun.serve({ port: 0, async fetch(request) {
    const data = await request.arrayBuffer(); requests++
    return Response.json({ authority_id: authority.authority_id, store: "clickhouse", committed: true,
      sha256: request.headers.get("x-vcc-sha256"), source_id: request.headers.get("x-vcc-source-id"),
      source_kind: request.headers.get("x-vcc-source-kind"), locator: request.headers.get("x-vcc-source-locator"),
      file_id: decodeURIComponent(new URL(request.url).pathname.slice("/file/".length)), path: request.headers.get("x-vcc-path"),
      start: Number(request.headers.get("x-vcc-start")), size: data.byteLength, full: true })
  } })
  try {
    const filename = path.join(root, "source.jsonl"); await writeFile(filename, "abcd")
    const input = { endpoint: server.url.toString(), token: "token", fileID: "conflict", sourcePath: "source.jsonl", filename, size: 4, start: 0, full: true, source: { id: "a".repeat(32), kind: "pi-jsonl", locator: root } }
    const outbox = outboxDirectory(authority); await mkdir(outbox, { recursive: true })
    const id = (file: string) => createHash("sha256").update(`${input.source.id}\n${file}.0`).digest("hex")
    const conflict = path.join(outbox, `${id("conflict")}.body`)
    await writeFile(conflict, "old!")
    await expect(uploadClickHouseFile(input, authority)).rejects.toThrow("conflicting orphaned outbox body")
    expect(await readFile(conflict, "utf8")).toBe("old!")
    const leftovers = (await readdir(outbox)).filter(name => name.startsWith(`.${id("conflict")}.`))
    expect(leftovers).toHaveLength(1)
    expect(await readFile(path.join(outbox, leftovers[0]!), "utf8")).toBe("abcd")
    expect(requests).toBe(0)
    const resumed = path.join(outbox, `${id("resume")}.body`)
    await writeFile(resumed, "abcd")
    await uploadClickHouseFile({ ...input, fileID: "resume" }, authority)
    expect(await readFile(resumed, "utf8")).toBe("abcd")
    expect(requests).toBe(1)
  } finally {
    server.stop(true)
    if (previous === undefined) delete process.env.BETTER_SYNC_OUTBOX
    else process.env.BETTER_SYNC_OUTBOX = previous
    await rm(root, { recursive: true, force: true })
  }
})

test("receipt with a different source kind or locator is not acknowledged", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "ch-receipt-"))
  const previous = process.env.BETTER_SYNC_OUTBOX
  process.env.BETTER_SYNC_OUTBOX = root
  const server = Bun.serve({ port: 0, async fetch(request) {
    const data = await request.arrayBuffer()
    return Response.json({ authority_id: authority.authority_id, store: "clickhouse", committed: true,
      sha256: request.headers.get("x-vcc-sha256"), source_id: request.headers.get("x-vcc-source-id"),
      source_kind: "wrong-kind", locator: "wrong-root", file_id: decodeURIComponent(new URL(request.url).pathname.slice("/file/".length)),
      path: request.headers.get("x-vcc-path"), start: Number(request.headers.get("x-vcc-start")), size: data.byteLength, full: true })
  } })
  try {
    const filename = path.join(root, "source.jsonl"); await writeFile(filename, "abcd")
    const input = { endpoint: server.url.toString(), token: "token", fileID: "file", sourcePath: "source.jsonl", filename, size: 4, start: 0, full: true, source: { id: "a".repeat(32), kind: "pi-jsonl", locator: root } }
    await expect(uploadClickHouseFile(input, authority)).rejects.toThrow("invalid ClickHouse commit receipt")
    expect(await clickHouseOutboxStats(authority)).toMatchObject({ pending: 1, retained: 0 })
  } finally {
    server.stop(true)
    if (previous === undefined) delete process.env.BETTER_SYNC_OUTBOX
    else process.env.BETTER_SYNC_OUTBOX = previous
    await rm(root, { recursive: true, force: true })
  }
})

test("retained payloads count toward backpressure and are never released on ACK", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "ch-limit-"))
  const previous = process.env.BETTER_SYNC_OUTBOX
  process.env.BETTER_SYNC_OUTBOX = root
  let requests = 0
  const server = Bun.serve({ port: 0, async fetch(request) {
    const data = await request.arrayBuffer(); requests++
    return Response.json({ authority_id: authority.authority_id, store: "clickhouse", committed: true,
      sha256: request.headers.get("x-vcc-sha256"), source_id: request.headers.get("x-vcc-source-id"), source_kind: request.headers.get("x-vcc-source-kind"), locator: request.headers.get("x-vcc-source-locator"),
      file_id: decodeURIComponent(new URL(request.url).pathname.slice("/file/".length)), path: request.headers.get("x-vcc-path"),
      start: Number(request.headers.get("x-vcc-start")), size: data.byteLength, full: request.headers.get("x-vcc-full") === "true" })
  } })
  try {
    const filename = path.join(root, "source.jsonl")
    await writeFile(filename, "abcd")
    const input = { endpoint: server.url.toString(), token: "token", fileID: "file", sourcePath: "source.jsonl", filename, size: 4, start: 0, full: true, source: { id: "a".repeat(32), kind: "pi-jsonl", locator: root } }
    await uploadClickHouseFile(input, authority, 4096)
    const stats = await clickHouseOutboxStats(authority)
    expect(stats.retained).toBe(1)
    expect(stats.bytes).toBeGreaterThan(4)
    await uploadClickHouseFile(input, authority, 4096) // committed replay is idempotent
    expect(requests).toBe(1)
    await expect(uploadClickHouseFile({ ...input, fileID: "different" }, authority, stats.bytes + 1)).rejects.toThrow("outbox limit")
    expect(requests).toBe(1)
    expect(await clickHouseOutboxStats(authority)).toMatchObject({ pending: 0, retained: 1, bytes: stats.bytes })
    const body = (await readdir(outboxDirectory(authority))).find(name => name.endsWith(".body"))!
    expect(await readFile(path.join(outboxDirectory(authority), body), "utf8")).toBe("abcd")
  } finally {
    server.stop(true)
    if (previous === undefined) delete process.env.BETTER_SYNC_OUTBOX
    else process.env.BETTER_SYNC_OUTBOX = previous
    await rm(root, { recursive: true, force: true })
  }
})
