import { createHash, randomUUID } from "node:crypto"
import { link, mkdir, open, readdir, readFile, rename, unlink } from "node:fs/promises"
import path from "node:path"
import { clickHouseOutboxStats, outboxDirectory, replayClickHouseSegment, type Authority } from "./clickhouse-sync.js"

const sha = (value: string | Uint8Array) => createHash("sha256").update(value).digest("hex")
export type GenerationIntent = {
  authority: string
  source_id: string
  file_id: string
  path: string
  start: number
  size: number
  sha256: string
  parent: string | null
  predecessor: string | null
  mtime_ms: number
  segment_bytes: number
  created_at: number
  committed_at?: number
  applied_at?: number
}

type Segment = { authority: string; fileID: string; sourcePath: string; start: number; full: boolean; source: { id: string }; size: number; sha256: string; committed_at?: number }

async function syncDir(root: string) { const fd = await open(root, "r"); try { await fd.sync() } finally { await fd.close() } }
async function atomicJSON(root: string, target: string, value: unknown) {
  const temporary = path.join(root, `.generation-${randomUUID()}`)
  const fd = await open(temporary, "wx", 0o600)
  try { await fd.writeFile(JSON.stringify(value)); await fd.sync() } finally { await fd.close() }
  await rename(temporary, target)
  await syncDir(root)
}
function filename(root: string, intent: GenerationIntent) {
  return path.join(root, `${sha(`${intent.source_id}\n${intent.file_id}`)}.generation.json`)
}
function immutable(intent: GenerationIntent) {
  const { committed_at: _committed, applied_at: _applied, created_at: _created, ...value } = intent
  return JSON.stringify(value)
}
function validate(intent: GenerationIntent, authority: Authority) {
  if (intent.authority !== authority.authority_id || !/^[a-f0-9]{16,64}$/.test(intent.source_id) ||
      !intent.file_id || !intent.path || !/^[a-f0-9]{64}$/.test(intent.sha256) ||
      !Number.isSafeInteger(intent.start) || !Number.isSafeInteger(intent.size) || intent.start < 0 || intent.size < intent.start ||
      !Number.isSafeInteger(intent.segment_bytes) || intent.segment_bytes < 1 || intent.segment_bytes > 8*1024*1024 ||
      !Number.isFinite(intent.mtime_ms) || !Number.isSafeInteger(intent.created_at) || intent.created_at <= 0 ||
      (intent.parent !== null && (typeof intent.parent !== "string" || !intent.parent || intent.parent !== intent.predecessor)) ||
      (intent.predecessor !== null && (typeof intent.predecessor !== "string" || !intent.predecessor)) ||
      (intent.parent === null && intent.start !== 0) ||
      (intent.applied_at !== undefined && intent.committed_at === undefined)) throw new Error("invalid generation outbox intent")
  if (intent.size > 256*1024*1024 || Math.max(1,Math.ceil((intent.size-intent.start)/intent.segment_bytes)) > 4096) {
    throw new Error("generation exceeds bounded receiver verification limit")
  }
  for (const timestamp of [intent.committed_at, intent.applied_at]) {
    if (timestamp !== undefined && (!Number.isSafeInteger(timestamp) || timestamp <= 0)) throw new Error("invalid generation outbox timestamp")
  }
}

/** Persist a generation intent before sending any of its segments. */
export async function stageGeneration(intent: GenerationIntent, authority: Authority, maxBytes: number): Promise<void> {
  validate(intent, authority)
  const root = outboxDirectory(authority)
  await mkdir(root, { recursive: true, mode: 0o700 })
  const target = filename(root, intent)
  const existing = await readFile(target, "utf8").catch((e: NodeJS.ErrnoException) => { if (e.code === "ENOENT") return undefined; throw e })
  if (existing !== undefined) {
    const old = JSON.parse(existing) as GenerationIntent
    validate(old, authority)
    if (immutable(old) !== immutable(intent)) throw new Error("immutable generation intent conflict")
    return
  }
  const used = (await clickHouseOutboxStats(authority)).bytes
  const estimate = Buffer.byteLength(JSON.stringify({ ...intent, committed_at: Date.now(), applied_at: Date.now() })) * 2
  if (used + estimate > maxBytes) throw new Error("ClickHouse outbox limit reached; generation intent retained for retry")
  // Create without replacement: independent state databases can share this
  // authority outbox. Concurrent first writers must not overwrite an intent.
  const temporary = path.join(root, `.generation-${randomUUID()}`)
  const fd = await open(temporary, "wx", 0o600)
  try { await fd.writeFile(JSON.stringify(intent)); await fd.sync() } finally { await fd.close() }
  try {
    await link(temporary, target)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error
    const old = JSON.parse(await readFile(target, "utf8")) as GenerationIntent
    validate(old, authority)
    if (immutable(old) !== immutable(intent)) throw new Error("immutable generation intent conflict; preserving both intents")
  }
  await unlink(temporary)
  await syncDir(root)
}

/** The intent may precede segment capture; do not submit an incomplete file. */
async function segmentsReady(root: string, intent: GenerationIntent): Promise<boolean> {
  let position = intent.start
  do {
    const length = Math.min(intent.segment_bytes, intent.size - position)
    const fileID = `${intent.file_id}.${position}`
    const id = sha(`${intent.source_id}\n${fileID}`)
    const descriptor = await readFile(path.join(root, `${id}.json`), "utf8").catch((e: NodeJS.ErrnoException) => { if (e.code === "ENOENT") return undefined; throw e })
    if (descriptor === undefined) return false
    const segment = JSON.parse(descriptor) as Segment
    if (segment.authority !== intent.authority || segment.source?.id !== intent.source_id || segment.fileID !== fileID ||
        segment.sourcePath !== intent.path || segment.start !== position || segment.size !== length ||
        segment.full !== (intent.parent === null && position === 0)) throw new Error("generation segment identity mismatch")
    if (!Number.isSafeInteger(segment.committed_at) || segment.committed_at! <= 0) return false
    const data = await readFile(path.join(root, `${id}.body`))
    if (data.length !== length || sha(data) !== segment.sha256) throw new Error("generation outbox segment checksum mismatch; preserving bytes")
    position += length
  } while (position < intent.size)
  return true
}

export async function generationCheckpointed(authority: Authority, sourceID: string, fileID: string,
  expected?: { path: string; size: number; sha256: string }): Promise<boolean> {
  const root = outboxDirectory(authority)
  const target = path.join(root, `${sha(`${sourceID}\n${fileID}`)}.generation.json`)
  const value = await readFile(target, "utf8").catch((e: NodeJS.ErrnoException) => { if (e.code === "ENOENT") return undefined; throw e })
  if (value === undefined) return false
  const intent = JSON.parse(value) as GenerationIntent
  validate(intent, authority)
  if (intent.source_id !== sourceID || intent.file_id !== fileID || filename(root,intent) !== target ||
      (expected && (intent.path !== expected.path || intent.size !== expected.size || intent.sha256 !== expected.sha256))) {
    throw new Error("generation checkpoint identity mismatch")
  }
  return intent.applied_at !== undefined
}

export async function generationOutboxStats(authority: Authority): Promise<{ pending: number; committed: number; applied: number }> {
  const root = outboxDirectory(authority)
  const names = await readdir(root).catch((e: NodeJS.ErrnoException) => { if (e.code === "ENOENT") return []; throw e })
  const result = { pending: 0, committed: 0, applied: 0 }
  for (const name of names) {
    if (!/^[a-f0-9]{64}\.generation\.json$/.test(name)) continue
    const intent = JSON.parse(await readFile(path.join(root, name), "utf8")) as GenerationIntent
    validate(intent, authority)
    if (filename(root, intent) !== path.join(root, name)) throw new Error("generation intent filename mismatch")
    if (intent.applied_at !== undefined) result.applied++
    else if (intent.committed_at !== undefined) result.committed++
    else result.pending++
  }
  return result
}

type Destination = { endpoint: string; token: string; signal?: AbortSignal }
function generationBody(intent: GenerationIntent) {
  return { source_id: intent.source_id, file_id: intent.file_id, path: intent.path, start: intent.start,
    size: intent.size, sha256: intent.sha256, parent: intent.parent }
}
function checkReceipt(receipt: Record<string, unknown>, authority: Authority, body: Record<string, unknown>) {
  if (receipt.store !== "clickhouse" || receipt.authority_id !== authority.authority_id || receipt.committed !== true ||
      Object.entries(body).some(([key,value]) => receipt[key] !== value)) throw new Error("invalid ClickHouse generation receipt; durable intent retained")
}
async function postGeneration(input: Destination, authority: Authority, intent: GenerationIntent) {
  const body = generationBody(intent)
  const response = await fetch(`${input.endpoint.replace(/\/$/, "")}/generation`, {
    method: "POST", headers: { Authorization: `Bearer ${input.token}`, "content-type": "application/json" },
    body: JSON.stringify(body), signal: input.signal ?? AbortSignal.timeout(120_000),
  })
  if (!response.ok) throw new Error(`ClickHouse generation HTTP ${response.status}; durable intent retained`)
  checkReceipt(await response.json() as Record<string, unknown>, authority, body)
}

const versionKey = (source: string, file: string) => `${source}\n${file}`
async function orderedIntents(root: string, authority: Authority) {
  const names = (await readdir(root)).filter(name => /^[a-f0-9]{64}\.generation\.json$/.test(name))
  const intents = await Promise.all(names.map(async name => ({ target: path.join(root, name), intent: JSON.parse(await readFile(path.join(root, name), "utf8")) as GenerationIntent })))
  intents.sort((a,b) => a.intent.created_at - b.intent.created_at || a.target.localeCompare(b.target))
  for (const entry of intents) {
    validate(entry.intent, authority)
    if (filename(root, entry.intent) !== entry.target) throw new Error("generation intent filename mismatch")
  }
  // Wall clocks can jump and intents can share a millisecond. Order local
  // predecessor dependencies explicitly, not by timestamp or hash filename.
  const byVersion = new Map(intents.map(entry => [versionKey(entry.intent.source_id, entry.intent.file_id), entry]))
  const children = new Map<typeof intents[number], typeof intents>()
  const waiting = new Set<typeof intents[number]>()
  for (const entry of intents) {
    const prior = entry.intent.predecessor === null ? undefined : byVersion.get(versionKey(entry.intent.source_id, entry.intent.predecessor))
    if (prior) {
      if (prior.intent.path !== entry.intent.path) throw new Error("generation predecessor path mismatch")
      waiting.add(entry)
      children.set(prior, [...(children.get(prior) ?? []), entry])
    }
  }
  const ordered = intents.filter(entry => !waiting.has(entry))
  for (let index=0; index<ordered.length; index++) for (const child of children.get(ordered[index]!) ?? []) ordered.push(child)
  if (ordered.length !== intents.length) throw new Error("cyclic generation outbox predecessor chain")
  return { ordered, byVersion }
}

/** Replay committed generation receipts and apply local checkpoints idempotently. */
export async function drainGenerations(input: { endpoint: string; token: string; signal?: AbortSignal }, authority: Authority,
  apply: (intent: GenerationIntent) => void): Promise<{ pending: number; applied: number }> {
  const root = outboxDirectory(authority)
  await mkdir(root, { recursive: true, mode: 0o700 })
  const { ordered, byVersion } = await orderedIntents(root, authority)
  let pending = 0, applied = 0
  for (const entry of ordered) {
    const intent = entry.intent
    if (intent.applied_at !== undefined) continue
    const prior = intent.predecessor === null ? undefined : byVersion.get(versionKey(intent.source_id, intent.predecessor))
    if (prior && prior.intent.applied_at === undefined) { pending++; continue }
    if (intent.committed_at === undefined) {
      if (!(await segmentsReady(root, intent))) { pending++; continue }
      await postGeneration(input, authority, intent)
      intent.committed_at = Date.now()
      await atomicJSON(root, entry.target, intent)
    }
    // Caller must durably commit its checkpoint before returning. If the
    // process dies before the applied marker, replay calls apply again.
    apply(intent)
    intent.applied_at = Date.now()
    await atomicJSON(root, entry.target, intent)
    applied++
  }
  return { pending, applied }
}

async function generationRawVerified(input: Destination, authority: Authority, intent: GenerationIntent): Promise<boolean> {
  const item = { source_id: intent.source_id, file_id: intent.file_id, path: intent.path, size: intent.size, sha256: intent.sha256 }
  const response = await fetch(`${input.endpoint.replace(/\/$/, "")}/verify`, {
    method: "POST", headers: { Authorization: `Bearer ${input.token}`, "content-type": "application/json" },
    body: JSON.stringify({ files: [item] }), signal: input.signal ?? AbortSignal.timeout(120_000),
  })
  if (!response.ok) throw new Error(`ClickHouse generation verification HTTP ${response.status}; retained data preserved`)
  const result = await response.json() as { store?: unknown; authority_id?: unknown; files?: Array<{ file_id?: unknown; archive?: { status?: unknown } }> }
  if (result.store !== "clickhouse" || result.authority_id !== authority.authority_id || result.files?.length !== 1 ||
      result.files[0]?.file_id !== intent.file_id || typeof result.files[0]?.archive?.status !== "string") {
    throw new Error("invalid ClickHouse generation verification response")
  }
  return result.files[0]!.archive!.status === "verified"
}

/** Reconcile previously applied generations after a receiver/database restore.
 * Never delete outbox data: a missing receipt or failed raw verification is
 * repaired by replaying checksum-validated retained segments, then reverified.
 * This is not backup-inclusion proof and does not release any local bytes. */
export async function reconcileAppliedGenerations(input: Destination, authority: Authority): Promise<{ checked: number; repaired: number }> {
  if (authority.generation_status_protocol !== 1) throw new Error("ClickHouse receiver lacks generation recovery protocol")
  const root = outboxDirectory(authority)
  await mkdir(root, { recursive: true, mode: 0o700 })
  const { ordered, byVersion } = await orderedIntents(root, authority)
  const applied = ordered.filter(entry => entry.intent.applied_at !== undefined)
  const repaired = new Set<string>()
  async function replay(intent: GenerationIntent) {
    if (!(await segmentsReady(root,intent))) throw new Error("retained generation segments incomplete; refusing recovery")
    let position=intent.start
    do {
      await replayClickHouseSegment(input,authority,intent.source_id,`${intent.file_id}.${position}`)
      position+=Math.min(intent.segment_bytes,intent.size-position)
    } while (position<intent.size)
    repaired.add(versionKey(intent.source_id,intent.file_id))
  }
  // First restore missing receipts in predecessor order. A successful generation
  // insertion itself verifies its full ancestry at the receiver.
  for (const { intent } of applied) {
    const status = await fetch(`${input.endpoint.replace(/\/$/, "")}/generation/status`, {
      method: "POST", headers: { Authorization: `Bearer ${input.token}`, "content-type": "application/json" },
      body: JSON.stringify({ source_id: intent.source_id, file_id: intent.file_id }),
      signal: input.signal ?? AbortSignal.timeout(30_000),
    })
    if (status.status === 200) checkReceipt(await status.json() as Record<string, unknown>, authority, generationBody(intent))
    else if (status.status === 404) { await replay(intent); await postGeneration(input,authority,intent) }
    else throw new Error(`ClickHouse generation status HTTP ${status.status}; retained data preserved`)
  }
  // Verify only lineage leaves, not every prefix: leaf reconstruction checks
  // each intermediate hash. This avoids quadratic full-file re-reads on long
  // append chains. Full rewrites are separate roots and remain separate leaves.
  const parents = new Set(applied.flatMap(({intent}) => intent.parent === null ? [] : [versionKey(intent.source_id,intent.parent)]))
  for (const { intent } of applied) {
    if (parents.has(versionKey(intent.source_id,intent.file_id))) continue
    if (await generationRawVerified(input,authority,intent)) continue
    const chain: GenerationIntent[] = []
    let cursor: GenerationIntent | undefined = intent
    while (cursor) {
      chain.push(cursor)
      const ancestor: typeof ordered[number] | undefined = cursor.parent === null ? undefined : byVersion.get(versionKey(cursor.source_id,cursor.parent))
      cursor = ancestor?.intent.applied_at !== undefined ? ancestor.intent : undefined
    }
    for (const version of chain.reverse()) await replay(version)
    if (!(await generationRawVerified(input,authority,intent))) throw new Error("replayed generation failed raw verification; retained data preserved")
  }
  return { checked:applied.length,repaired:repaired.size }
}
