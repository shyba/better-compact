import type { SessionEntry } from "@earendil-works/pi-coding-agent"
import { redact, sha256, truncateUtf8, utf8Bytes } from "./ledger.js"
import { createVccArchiveHandle, parseVccArchiveHandle, verifyVccArchive } from "./vcc-archive.js"
import { canonicalSerialize } from "./vcc.js"

export const VCC_PI_RECALL_DEFAULT_MAX_RESULTS = 5
export const VCC_PI_RECALL_MAX_RESULTS = 16
export const VCC_PI_RECALL_DEFAULT_MAX_BYTES = 16 * 1_024
export const VCC_PI_RECALL_MAX_BYTES = 64 * 1_024
const VCC_PI_RECALL_MAX_ENTRIES = 4_096
const VCC_PI_RECALL_SOURCE_MAX_BYTES = 64 * 1_024
const VCC_PI_RECALL_PAYLOAD_MAX_BYTES = 16 * 1_024

export type VccPiRecallEntry = {
  handle: string
  entry_id: string
  parent_id: string | null
  entry_type: "message" | "custom_message"
  timestamp: string
  source_location: string
  source_bytes: string
  payload_bytes: string
  source_digest: string
  payload_digest: string
  searchable_text: string
  /** True for vcc_recall tool invocations and results: excluded from discovery
   *  by default so recall traffic stops contaminating its own results. */
  recall_traffic: boolean
}

export type VccPiRecallIndex = {
  session_id: string
  lineage_id: string
  complete: boolean
  reason: "page_limit" | "duplicate_message_id" | "unsupported_record" | null
  entries: VccPiRecallEntry[]
  /** Coverage of the active branch: recall serves the most recent window of a
   *  long session instead of refusing, so callers can see what they are missing. */
  branch_entries: number
  indexed_entries: number
  truncated: boolean
  /** Recall-eligible entries dropped for an unusable identity or a duplicate id.
   *  They must not disable recall for the whole session. */
  skipped_entries: number
}

export type VccPiRecallFailure =
  | "malformed_handle"
  | "scope_mismatch"
  | "handle_unavailable"
  | "digest_mismatch"
  | "oversized"

export type VccPiRecallResolution =
  | { ok: true; entry: VccPiRecallEntry }
  | { ok: false; reason: VccPiRecallFailure }

export type VccPiRecallToolResult = {
  title: string
  output: string
  metadata: { status: string }
}

/** The active branch is the only durable Pi source exposed by this V1 seam.
 * The first child is a stable fork marker: appending entries preserves it,
 * while sibling branches after the same root receive different markers. */
export function vccPiLineageId(session_id: string, branch: readonly SessionEntry[]) {
  const branch_marker = branch[1]?.id ?? branch[0]?.id ?? "empty-branch"
  return `pi-v1:${sha256(canonicalSerialize({ version: 1, session_id, branch_marker }))}`
}

/** Serialized shapes that are vcc_recall tool traffic (an invocation carries
 *  the tool name, a result carries our own kind marker; results embedded in a
 *  message part arrive JSON-escaped). */
const RECALL_TRAFFIC = /"(?:name|kind)"\s*:\s*"vcc_recall"|\\+"kind\\+"\s*:\s*\\+"vcc_recall\\+"/u

function indexRecallEntry(args: {
  entry: Extract<SessionEntry, { type: "message" | "custom_message" }>
  session_id: string
  lineage_id: string
  seen: Set<string>
  include_metadata?: boolean
}): VccPiRecallEntry | null {
  const { entry, session_id, lineage_id, seen, include_metadata = false } = args
  if (!validIdentity(entry.id)) return null
  if (seen.has(entry.id)) return null
  seen.add(entry.id)
  try {
    const projection = entryProjection(entry, include_metadata)
    const source_json = canonicalSerialize({ version: 1, kind: "pi-v1-source-entry", entry: projection })
    const source_bytes = boundedRedacted(source_json, VCC_PI_RECALL_SOURCE_MAX_BYTES)
    const source_location = `pi-v1:branch:${entry.id}`
    const payload_bytes = boundedRedacted(canonicalSerialize({
      version: 1,
      host: "pi",
      session_id,
      lineage_id,
      source_entry_id: entry.id,
      source_location,
      entry: projection,
    }), VCC_PI_RECALL_PAYLOAD_MAX_BYTES)
    const handle = createVccArchiveHandle({
      host: "pi",
      session_id,
      lineage_id,
      stable_source_id: entry.id,
      canonical_source_kind: "pi-session-entry",
      stable_source_identity: entry.id,
      source_bytes,
      payload_bytes,
    }).handle
    const verified = verifyVccArchive({
      handle,
      host: "pi",
      session_id,
      lineage_id,
      stable_source_id: entry.id,
      canonical_source_kind: "pi-session-entry",
      stable_source_identity: entry.id,
      source_bytes,
      payload_bytes,
    })
    if (!verified.valid) return null
    return {
      handle,
      entry_id: entry.id,
      parent_id: entry.parentId,
      entry_type: entry.type,
      timestamp: entry.timestamp,
      source_location,
      source_bytes,
      payload_bytes,
      source_digest: verified.source_digest,
      payload_digest: verified.payload_digest,
      searchable_text: payload_bytes.toLowerCase(),
      recall_traffic: RECALL_TRAFFIC.test(source_json),
    }
  } catch {
    return null
  }
}

/** Single pass instead of the old some(findIndex) quadratic scan: ids are
 *  counted once and any repeat marks the branch duplicated. */
function branchHasDuplicateIds(branch: readonly SessionEntry[]) {
  const counts = new Map<unknown, number>()
  for (const entry of branch) {
    const next = (counts.get(entry.id) ?? 0) + 1
    if (next > 1) return true
    counts.set(entry.id, next)
  }
  return false
}

export function buildVccPiRecallIndex(input: { session_id: string; branch: readonly SessionEntry[]; include_metadata?: boolean }): VccPiRecallIndex {
  return indexBranch(input.session_id, input.branch, undefined, input.include_metadata ?? false)
}

/** Repeated vcc_recall calls rebuilt and re-hashed the whole window every time
 *  (seconds of CPU per call). The branch is append-only in practice, so the
 *  index is kept per session and grown incrementally; a rewritten branch falls
 *  back to a full rebuild. */
type RecallIndexCacheEntry = {
  include_metadata: boolean
  lineage_id: string
  branch_len: number
  boundary_id: string | undefined
  window_start: number
  positions: number[]
  entries: VccPiRecallEntry[]
  seen: Set<string>
}
const recallIndexCache = new Map<string, RecallIndexCacheEntry>()

export function getVccPiRecallIndex(input: { session_id: string; branch: readonly SessionEntry[]; include_metadata?: boolean }): VccPiRecallIndex {
  return indexBranch(input.session_id, input.branch, recallIndexCache, input.include_metadata ?? false)
}

function indexBranch(session_id: string, branch: readonly SessionEntry[], cache: Map<string, RecallIndexCacheEntry> | undefined, include_metadata = false): VccPiRecallIndex {
  const lineage_id = vccPiLineageId(session_id, branch)
  const truncated = branch.length > VCC_PI_RECALL_MAX_ENTRIES
  const window_start = truncated ? branch.length - VCC_PI_RECALL_MAX_ENTRIES : 0
  const cached = cache?.get(session_id)
  const reusable = cached !== undefined
    && cached.include_metadata === include_metadata
    && cached.lineage_id === lineage_id
    && branch.length >= cached.branch_len
    && (cached.branch_len === 0 || branch[cached.branch_len - 1]?.id === cached.boundary_id)
  let positions: number[]
  let entries: VccPiRecallEntry[]
  let seen: Set<string>
  let hasInvalidAuthoritativeEntry = false
  if (reusable) {
    seen = cached.seen
    positions = []
    entries = []
    // keep only entries still inside the sliding tail window
    for (let i = 0; i < cached.entries.length; i++) {
      if (cached.positions[i]! >= window_start) {
        positions.push(cached.positions[i]!)
        entries.push(cached.entries[i]!)
      }
    }
    hasInvalidAuthoritativeEntry = branch.slice(0, cached.branch_len).some((entry) => isRecallEntry(entry) && !validIdentity(entry.id))
    for (let index = cached.branch_len; index < branch.length; index++) {
      const entry = branch[index]!
      if (!isRecallEntry(entry)) continue
      if (!validIdentity(entry.id)) {
        hasInvalidAuthoritativeEntry = true
        continue
      }
      const indexed = indexRecallEntry({ entry, session_id, lineage_id, seen, include_metadata })
      if (indexed && index >= window_start) {
        positions.push(index)
        entries.push(indexed)
      }
    }
  } else {
    seen = new Set<string>()
    positions = []
    entries = []
    hasInvalidAuthoritativeEntry = branch.some((entry) => isRecallEntry(entry) && !validIdentity(entry.id))
    for (let index = 0; index < branch.length; index++) {
      const entry = branch[index]!
      if (!isRecallEntry(entry)) continue
      const indexed = indexRecallEntry({ entry, session_id, lineage_id, seen, include_metadata })
      if (indexed && index >= window_start) {
        positions.push(index)
        entries.push(indexed)
      }
    }
  }
  const window = branch.slice(window_start)
  const duplicate = branchHasDuplicateIds(branch)
  const recallEligible = window.filter((entry) => isRecallEntry(entry)).length
  const complete = !truncated
  const reason = !complete ? "page_limit" : hasInvalidAuthoritativeEntry ? "unsupported_record" : null
  if (cache) {
    if (cache.size > 8) cache.clear()
    cache.set(session_id, {
      include_metadata,
      lineage_id,
      branch_len: branch.length,
      boundary_id: branch.at(-1)?.id,
      window_start,
      positions,
      entries,
      seen,
    })
  }
  return {
    session_id,
    lineage_id,
    complete: complete && !duplicate && !hasInvalidAuthoritativeEntry,
    reason: duplicate ? "duplicate_message_id" : reason,
    entries,
    branch_entries: branch.length,
    indexed_entries: entries.length,
    truncated,
    skipped_entries: Math.max(0, recallEligible - entries.length),
  }
}

/** Bounded readable excerpt around the strongest query match: discovery used
 *  to hand back handles and digests with no evidence of why a result matched. */
export function recallEntrySnippet(entry: VccPiRecallEntry, tokens: readonly string[], max_bytes = 320) {
  const lower = entry.searchable_text
  let at = -1
  for (const token of tokens) {
    const found = lower.indexOf(token)
    if (found >= 0 && (at < 0 || found < at)) at = found
  }
  if (at < 0) return truncateUtf8(entry.payload_bytes, max_bytes)
  const start = Math.max(0, at - Math.floor(max_bytes / 3))
  const prefix = start > 0 ? "..." : ""
  return prefix + truncateUtf8(entry.payload_bytes.slice(start), max_bytes)
}

export function resolveVccPiHandle(input: {
  handle: string
  session_id: string
  lineage_id: string
  entries: readonly VccPiRecallEntry[]
  max_bytes?: number
}): VccPiRecallResolution {
  const parts = parseVccArchiveHandle(input.handle)
  if (!parts) return { ok: false, reason: "malformed_handle" }
  if (parts.host !== "pi") return { ok: false, reason: "scope_mismatch" }
  if (parts.scope_digest !== sha256(`vcc-scope-v1\0pi\0${input.session_id}\0${input.lineage_id}`)) return { ok: false, reason: "scope_mismatch" }
  const entry = input.entries.find((candidate) => candidate.handle === input.handle)
  if (!entry) return { ok: false, reason: "handle_unavailable" }
  if (input.max_bytes !== undefined && utf8Bytes(entry.payload_bytes) > input.max_bytes) return { ok: false, reason: "oversized" }
  const verified = verifyVccArchive({
    handle: entry.handle,
    host: "pi",
    session_id: input.session_id,
    lineage_id: input.lineage_id,
    stable_source_id: entry.entry_id,
    canonical_source_kind: "pi-session-entry",
    stable_source_identity: entry.entry_id,
    source_bytes: entry.source_bytes,
    payload_bytes: entry.payload_bytes,
  })
  return verified.valid ? { ok: true, entry } : { ok: false, reason: verified.reason === "scope_mismatch" ? "scope_mismatch" : "digest_mismatch" }
}

export function discoverVccPiHandles(input: {
  query: string
  entries: readonly VccPiRecallEntry[]
  page?: number
  max_results?: number
  include_recall_traffic?: boolean
}) {
  const query = input.query.trim().toLowerCase()
  const tokens = query.split(/\s+/u).filter(Boolean)
  // Recall invocations and results are excluded unless requested: every call
  // otherwise matches its own previous queries and the result set degrades.
  const searchable = input.include_recall_traffic ? input.entries : input.entries.filter((entry) => !entry.recall_traffic)
  const page = Number.isSafeInteger(input.page) && input.page! > 0 ? input.page! : 1
  const max_results = boundedResults(input.max_results)
  // Rank by how many query terms an entry covers instead of demanding all of
  // them: multi-word recall queries are paraphrases, and requiring every term
  // in one redacted payload returned an empty page for ordinary questions.
  const ranked = searchable
    .map((entry) => ({ entry, score: tokens.reduce((total, token) => total + (entry.searchable_text.includes(token) ? 1 : 0), 0) }))
    .filter((candidate) => tokens.length > 0 && candidate.score > 0)
    .sort((left, right) => right.score - left.score || compareCodePoints(left.entry.entry_id, right.entry.entry_id))
  const start = (page - 1) * max_results
  const page_entries = ranked.slice(start, start + max_results).map((candidate) => candidate.entry)
  return {
    total: ranked.length,
    page,
    total_pages: Math.max(1, Math.ceil(ranked.length / max_results)),
    // Result pagination is distinct from window coverage: a short page here
    // says nothing about how much of the branch is indexed.
    results_truncated: start + page_entries.length < ranked.length,
    entries: page_entries,
  }
}

export function renderVccPiRecallEntry(entry: VccPiRecallEntry, max_bytes: number) {
  if (utf8Bytes(entry.payload_bytes) > max_bytes) return
  const output = canonicalSerialize({
    handle: entry.handle,
    entry_id: entry.entry_id,
    parent_id: entry.parent_id,
    entry_type: entry.entry_type,
    timestamp: entry.timestamp,
    source_location: entry.source_location,
    source_digest: entry.source_digest,
    payload_digest: entry.payload_digest,
    presentation: "transformed_redacted",
    payload: entry.payload_bytes,
  })
  if (utf8Bytes(output) > max_bytes) return
  return output
}

/** Fallback for oversized retrieval: a readable excerpt plus the budget the
 *  caller needs for the full payload, instead of a bare "oversized". */
export function recallEntryExcerpt(entry: VccPiRecallEntry, max_bytes: number) {
  const required_bytes = utf8Bytes(entry.payload_bytes)
  return {
    handle: entry.handle,
    entry_id: entry.entry_id,
    parent_id: entry.parent_id,
    entry_type: entry.entry_type,
    timestamp: entry.timestamp,
    source_location: entry.source_location,
    source_digest: entry.source_digest,
    payload_digest: entry.payload_digest,
    presentation: "excerpt_redacted",
    excerpt: truncateUtf8(entry.payload_bytes, Math.max(256, Math.min(max_bytes, VCC_PI_RECALL_DEFAULT_MAX_BYTES))),
    required_bytes,
    hint: `set max_bytes to at least ${required_bytes} and resolve the handle for the full payload`,
  }
}

export function vccPiRecallToolResult(status: string, body: Record<string, unknown>, max_bytes: number): VccPiRecallToolResult {
  const output = canonicalSerialize({ version: 1, kind: "vcc_recall", host: "pi", status, ...body })
  if (utf8Bytes(output) <= max_bytes) return { title: `VCC Pi recall: ${status}`, output, metadata: { status } }
  const bounded = canonicalSerialize({ version: 1, kind: "vcc_recall", host: "pi", status: "oversized", requested_bytes: max_bytes })
  return { title: "VCC Pi recall: oversized", output: bounded, metadata: { status: "oversized" } }
}

function isRecallEntry(entry: SessionEntry): entry is Extract<SessionEntry, { type: "message" | "custom_message" }> {
  return entry.type === "message" || (entry.type === "custom_message" && entry.display)
}

/** Provider bookkeeping and internal reasoning are excluded from recall
 *  payloads by default; include_metadata restores the raw record. The default
 *  projection is user-visible text and tool evidence only. */
const METADATA_KEYS = new Set(["usage", "providerMetadata", "provider_metadata", "details", "cost", "cache", "latency", "error", "metadata"])
const INTERNAL_BLOCK_TYPES = new Set(["thinking", "reasoning"])

function stripMetadataKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stripMetadataKeys)
  if (!isRecord(value)) return value
  const out: Record<string, unknown> = {}
  for (const [key, item] of Object.entries(value)) {
    if (METADATA_KEYS.has(key)) continue
    out[key] = stripMetadataKeys(item)
  }
  return out
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function visibleMessage(message: unknown): unknown {
  if (!isRecord(message)) return message
  const out: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(message)) {
    if (METADATA_KEYS.has(key)) continue
    if (key === "content" && Array.isArray(value)) {
      out[key] = value
        .filter((block) => isRecord(block) && !INTERNAL_BLOCK_TYPES.has(String(block.type)))
        .map(stripMetadataKeys)
      continue
    }
    out[key] = stripMetadataKeys(value)
  }
  return out
}

function entryProjection(entry: Extract<SessionEntry, { type: "message" | "custom_message" }>, include_metadata = false) {
  if (entry.type === "message") {
    return { id: entry.id, parentId: entry.parentId, timestamp: entry.timestamp, type: entry.type, message: include_metadata ? entry.message : visibleMessage(entry.message) }
  }
  return { id: entry.id, parentId: entry.parentId, timestamp: entry.timestamp, type: entry.type, customType: entry.customType, content: entry.content, display: entry.display }
}

function boundedRedacted(value: string, max_bytes: number) {
  return truncateUtf8(redact(value), max_bytes)
}

function validIdentity(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && !value.includes(":") && !value.includes("\0") && !value.includes("\n") && !/^\d+$/u.test(value) && utf8Bytes(value) <= 512
}

function boundedResults(value: number | undefined) {
  if (!Number.isSafeInteger(value)) return VCC_PI_RECALL_DEFAULT_MAX_RESULTS
  return Math.min(VCC_PI_RECALL_MAX_RESULTS, Math.max(1, value!))
}

function compareCodePoints(left: string, right: string) {
  const leftPoints = Array.from(left)
  const rightPoints = Array.from(right)
  for (let index = 0; index < Math.min(leftPoints.length, rightPoints.length); index++) {
    const difference = leftPoints[index]!.codePointAt(0)! - rightPoints[index]!.codePointAt(0)!
    if (difference !== 0) return difference
  }
  return leftPoints.length - rightPoints.length
}
