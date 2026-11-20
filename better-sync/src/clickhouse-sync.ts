import { createHash, randomUUID } from "node:crypto"
import { link, mkdir, open, readdir, readFile, rename, stat, unlink } from "node:fs/promises"
import path from "node:path"
import os from "node:os"
import type { S3Upload } from "./s3-sync.js"

export type Authority = { store: "clickhouse"; protocol: 1; authority_id: string; max_segment_bytes: number; generation_protocol?: number; generation_status_protocol?: number }
const sha = (value: string | Uint8Array) => createHash("sha256").update(value).digest("hex")

/** Fail closed on a broken capability response. Only an explicit old-server
 * 404 permits legacy behavior; an outage must not silently change authority. */
export async function discoverAuthority(endpoint: string, token: string, signal?: AbortSignal): Promise<Authority | undefined> {
  const response = await fetch(`${endpoint.replace(/\/$/, "")}/capabilities`, {
    headers: { Authorization: `Bearer ${token}` }, signal: signal ?? AbortSignal.timeout(10_000),
  })
  if (response.status === 404) return undefined
  if (!response.ok) throw new Error(`sync capabilities HTTP ${response.status}`)
  const info = await response.json() as Authority
  if (info.store !== "clickhouse" || info.protocol !== 1 || !/^[a-zA-Z0-9_-]{16,128}$/.test(info.authority_id) || !Number.isSafeInteger(info.max_segment_bytes) || info.max_segment_bytes < 1) throw new Error("invalid sync authority response")
  return info
}

export function authorityStatePath(state: string, authority?: Authority): string {
  return authority ? `${state}.clickhouse-${sha(authority.authority_id).slice(0, 24)}` : state
}

/** First write pins the selected authority. Never adopt a replacement authority
 * implicitly: the old ID must be restored with its data or deliberately migrated. */
export async function pinClickHouseAuthority(state: string, authority: Authority, create: boolean): Promise<void> {
  const pin = `${state}.clickhouse-authority.json`
  if (create) {
    await mkdir(path.dirname(pin), { recursive: true, mode: 0o700 })
    // Upgrade from the pre-pin implementation may already have namespaced
    // state. Never pin a newly advertised identity over another authority's
    // existing state, even when no pin file has yet been created.
    const prefix = `${path.basename(state)}.clickhouse-`
    const expected = path.basename(authorityStatePath(state, authority))
    const existingStates = (await readdir(path.dirname(pin))).filter(name => name.startsWith(prefix) && /^[a-f0-9]{24}$/.test(name.slice(prefix.length)))
    if (existingStates.some(name => name !== expected)) throw new Error("different ClickHouse authority state already exists; refusing implicit authority change")
    let file
    try { file = await open(pin, "wx", 0o600) } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error
    }
    if (file) {
      try { await file.writeFile(JSON.stringify({ store: authority.store, protocol: authority.protocol, authority_id: authority.authority_id })); await file.sync() } finally { await file.close() }
      await fsyncDirectory(path.dirname(pin))
    }
  }
  let stored: { store?: unknown; protocol?: unknown; authority_id?: unknown }
  try { stored = JSON.parse(await readFile(pin, "utf8")) as typeof stored } catch {
    throw new Error("ClickHouse authority pin missing or invalid; refusing implicit authority change")
  }
  if (stored.store !== authority.store || stored.protocol !== authority.protocol || stored.authority_id !== authority.authority_id) {
    throw new Error("ClickHouse authority changed; refusing implicit authority change")
  }
}

type Pending = { authority: string; fileID: string; sourcePath: string; start: number; full: boolean; source: { id: string; kind: string; locator: string }; size: number; sha256: string; committed_at?: number }
export type OutboxStats = { pending: number; retained: number; bytes: number; oldestRetainedAt?: number }

/** Include orphaned bodies and temporary files in the limit: uncertainty must
 * never make potentially recoverable bytes disappear or bypass backpressure. */
export async function clickHouseOutboxStats(authority: Authority): Promise<OutboxStats> {
  const root = outboxDirectory(authority)
  const names = await readdir(root).catch((error: NodeJS.ErrnoException) => { if (error.code === "ENOENT") return []; throw error })
  const result: OutboxStats = { pending: 0, retained: 0, bytes: 0 }
  for (const name of names) {
    const file = path.join(root, name)
    const info = await stat(file)
    if (info.isFile()) result.bytes += info.size
    if (/^[a-f0-9]{64}\.json$/.test(name)) {
      const meta = JSON.parse(await readFile(file, "utf8")) as Pending
      if (meta.authority !== authority.authority_id) throw new Error("outbox authority mismatch")
      if (meta.committed_at !== undefined) {
        if (!Number.isSafeInteger(meta.committed_at) || meta.committed_at <= 0) throw new Error("invalid outbox committed timestamp")
        result.retained++
        result.oldestRetainedAt = Math.min(result.oldestRetainedAt ?? meta.committed_at, meta.committed_at)
      } else result.pending++
    }
  }
  return result
}

async function fsyncDirectory(root: string) {
  const dir = await open(root, "r")
  try { await dir.sync() } finally { await dir.close() }
}

export function outboxDirectory(authority: Authority): string {
  return path.join(process.env.BETTER_SYNC_OUTBOX ?? path.join(os.homedir(), ".local/state/better-sync/outbox"), sha(authority.authority_id))
}

async function send(root: string, id: string, input: { endpoint: string; token: string; signal?: AbortSignal }, authority: Authority, replay = false) {
  const meta = JSON.parse(await readFile(path.join(root, `${id}.json`), "utf8")) as Pending
  if (meta.authority !== authority.authority_id) throw new Error("outbox authority mismatch")
  if (meta.committed_at !== undefined) {
    if (!Number.isSafeInteger(meta.committed_at) || meta.committed_at <= 0) throw new Error("invalid outbox committed timestamp")
    // Retained bytes are not released here. Rehash them when proving backup
    // inclusion or replaying, not on every daemon polling pass.
    if (!replay) return
  }
  const data = await readFile(path.join(root, `${id}.body`))
  if (data.length !== meta.size || sha(data) !== meta.sha256) throw new Error("outbox checksum mismatch; preserving files")
  const response = await fetch(`${input.endpoint.replace(/\/$/, "")}/file/${encodeURIComponent(meta.fileID)}`, {
    method: "PUT", headers: { Authorization: `Bearer ${input.token}`, "content-type": "application/octet-stream", "content-length": String(data.length),
      "x-vcc-source-id": meta.source.id, "x-vcc-source-kind": meta.source.kind, "x-vcc-source-locator": meta.source.locator,
      "x-vcc-path": meta.sourcePath, "x-vcc-start": String(meta.start), "x-vcc-full": String(meta.full), "x-vcc-sha256": meta.sha256 },
    body: data, signal: input.signal ?? AbortSignal.timeout(600_000),
  })
  if (!response.ok) throw new Error(`ClickHouse sync HTTP ${response.status}; durable outbox retained`)
  const receipt = await response.json() as Record<string, unknown>
  if (receipt.authority_id !== authority.authority_id || receipt.store !== "clickhouse" || receipt.committed !== true || receipt.sha256 !== meta.sha256 || receipt.source_id !== meta.source.id || receipt.source_kind !== meta.source.kind || receipt.locator !== meta.source.locator || receipt.file_id !== meta.fileID || receipt.start !== meta.start || receipt.size !== meta.size || receipt.path !== meta.sourcePath || receipt.full !== meta.full) throw new Error("invalid ClickHouse commit receipt; durable outbox retained")
  if (meta.committed_at !== undefined) return // replay verified; retain the original timestamp and bytes
  // A synchronous insert receipt is not backup durability. Keep the body and
  // atomically mark its descriptor committed; crash before rename just causes
  // an idempotent replay. No release path exists until independently verified
  // backup inclusion of the segment, references and chunks is implemented.
  const temporary = path.join(root, `.${id}.${randomUUID()}.committed`)
  const descriptor = await open(temporary, "wx", 0o600)
  try { await descriptor.writeFile(JSON.stringify({ ...meta, committed_at: Date.now() })); await descriptor.sync() } finally { await descriptor.close() }
  await rename(temporary, path.join(root, `${id}.json`))
  await fsyncDirectory(root)
}

export async function replayClickHouseSegment(input: { endpoint: string; token: string; signal?: AbortSignal }, authority: Authority, sourceID: string, fileID: string) {
  const root = outboxDirectory(authority)
  await send(root, sha(`${sourceID}\n${fileID}`), input, authority, true)
}

export async function drainClickHouseOutbox(input: { endpoint: string; token: string; signal?: AbortSignal }, authority: Authority) {
  const root = outboxDirectory(authority)
  await mkdir(root, { recursive: true, mode: 0o700 })
  for (const name of (await readdir(root)).sort()) {
    if (/^[a-f0-9]{64}\.json$/.test(name)) await send(root, name.slice(0, -5), input, authority)
  }
}

/** Segment at bounded byte ranges. Raw bytes are retained even when a trailing
 * record is partial; an event projection must wait for its completion. */
export async function uploadClickHouseFile(input: S3Upload, authority: Authority, maxOutboxBytes = Number.MAX_SAFE_INTEGER): Promise<{ status: number; size: number }> {
  if (!Number.isSafeInteger(maxOutboxBytes) || maxOutboxBytes <= 0) throw new Error("invalid outbox byte limit")
  if (!input.source) throw new Error("ClickHouse sync requires explicit source identity")
  const root = outboxDirectory(authority)
  await mkdir(root, { recursive: true, mode: 0o700 })
  const before = await stat(input.filename)
  const limit = Math.min(authority.max_segment_bytes, 8 * 1024 * 1024)
  let position = input.start
  do {
    const length = Math.min(limit, input.size - position)
    if (length < 0) throw new Error("invalid snapshot range")
    const data = Buffer.alloc(length)
    const handle = await open(input.filename, "r")
    try {
      let seen = 0
      while (seen < length) {
        const got = await handle.read(data, seen, length - seen, position + seen)
        if (!got.bytesRead) throw new Error("source truncated during outbox snapshot")
        seen += got.bytesRead
      }
    } finally { await handle.close() }
    const after = await stat(input.filename)
    if (after.size !== before.size || after.mtimeMs !== before.mtimeMs) throw new Error("source changed during snapshot; retry")
    const meta: Pending = { authority: authority.authority_id, fileID: `${input.fileID}.${position}`, sourcePath: input.sourcePath, start: position,
      full: input.full && position === 0, source: input.source, size: length, sha256: sha(data) }
    const id = sha(`${input.source.id}\n${meta.fileID}`)
    const descriptor = path.join(root, `${id}.json`)
    const existing = await readFile(descriptor, "utf8").catch((error: NodeJS.ErrnoException) => { if (error.code === "ENOENT") return undefined; throw error })
    if (existing) {
      const { committed_at: _committed, ...immutable } = JSON.parse(existing) as Pending
      if (JSON.stringify(immutable) !== JSON.stringify(meta)) throw new Error("immutable outbox identity conflict")
    }
    if (!existing) {
      const used = (await clickHouseOutboxStats(authority)).bytes
      // Account for payload, descriptor and the atomic committed-descriptor
      // replacement. Other files (including crash remnants) count in used.
      const descriptorBudget = Buffer.byteLength(JSON.stringify({ ...meta, committed_at: Date.now() })) * 2
      if (used + data.length + descriptorBudget > maxOutboxBytes) throw new Error("ClickHouse outbox limit reached; retained bytes await verified backup inclusion")
      const temporary = path.join(root, `.${id}.${randomUUID()}`)
      const spool = await open(temporary, "wx", 0o600)
      try { await spool.writeFile(data); await spool.sync() } finally { await spool.close() }
      const bodyPath = path.join(root, `${id}.body`)
      try {
        // A crash may have left a body without a descriptor. Never rename
        // over it: unknown retained bytes are not disposable.
        await link(temporary, bodyPath)
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error
        const orphan = await readFile(bodyPath)
        if (orphan.length !== data.length || sha(orphan) !== meta.sha256) throw new Error("conflicting orphaned outbox body; preserving both copies")
      }
      await unlink(temporary)
      await fsyncDirectory(root)
      const manifestPath = `${temporary}.json`
      const manifest = await open(manifestPath, "wx", 0o600)
      try { await manifest.writeFile(JSON.stringify(meta)); await manifest.sync() } finally { await manifest.close() }
      try {
        await link(manifestPath, descriptor)
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error
        const { committed_at: _committed, ...immutable } = JSON.parse(await readFile(descriptor, "utf8")) as Pending
        if (JSON.stringify(immutable) !== JSON.stringify(meta)) throw new Error("immutable outbox identity conflict; preserving descriptor")
      }
      await unlink(manifestPath)
      await fsyncDirectory(root)
    }
    await send(root, id, input, authority)
    position += length
  } while (position < input.size)
  return { status: 201, size: input.size - input.start }
}
