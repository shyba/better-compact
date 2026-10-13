// Text bounds and redaction used by the sync pipeline when mirroring session
// content. Copied from better-compact's ledger utilities on purpose: the two
// packages stay independently installable (a sync-only host carries no
// compaction code), so each ships its own tiny text core instead of a
// cross-package dependency. Keep the implementations behaviorally identical.

export function utf8Bytes(value: string) {
  let bytes = 0
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index)
    if (code <= 0x7f) {
      bytes++
      continue
    }
    if (code <= 0x7ff) {
      bytes += 2
      continue
    }
    if (hasSurrogatePair(value, index)) {
      bytes += 4
      index++
      continue
    }
    bytes += 3
  }
  return bytes
}

export function truncateUtf8(value: string, maxBytes: number) {
  if (utf8Bytes(value) <= maxBytes) return value
  const marker = "…[truncated]"
  const markerBytes = utf8Bytes(marker)
  if (maxBytes <= markerBytes) return marker.slice(0, utf8PrefixEnd(marker, Math.max(0, maxBytes)))
  const budget = maxBytes - markerBytes
  return value.slice(0, utf8PrefixEnd(value, budget)) + marker
}

export function redact(value: string) {
  return value
    .replace(
      /(["']?(?:[a-z0-9_-]{0,64}(?:api[_-]?key|password|passwd|secret|token|private[_-]?key|access[_-]?key(?:[_-]?id)?|credential)|authorization)["']?\s*[:=]\s*)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')/gi,
      "$1[REDACTED]",
    )
    .replace(
      /(^|[\s{,?&])(["']?(?:[a-z0-9_-]{0,64}(?:api[_-]?key|password|passwd|secret|token|private[_-]?key|access[_-]?key(?:[_-]?id)?|credential)|authorization)["']?\s*[:=]\s*)(?:(?:bearer|basic)\s+)?[^\s,}&]+/gim,
      "$1$2[REDACTED]",
    )
    .replace(/\b(sk-[A-Za-z0-9_-]{12,}|ghp_[A-Za-z0-9]{12,}|github_pat_[A-Za-z0-9_]{12,}|AKIA[A-Z0-9]{16})\b/g, "[REDACTED]")
    .replace(/\b(?![a-f0-9]{40,}\b)[A-Za-z0-9+/]{40,}={0,2}\b/gi, "[REDACTED]")
}

function utf8PrefixEnd(value: string, maxBytes: number) {
  let bytes = 0
  let index = 0
  while (index < value.length) {
    const code = value.charCodeAt(index)
    const surrogatePair = hasSurrogatePair(value, index)
    const size = code <= 0x7f ? 1 : code <= 0x7ff ? 2 : surrogatePair ? 4 : 3
    if (bytes + size > maxBytes) break
    bytes += size
    index += surrogatePair ? 2 : 1
  }
  return index
}

function hasSurrogatePair(value: string, index: number) {
  const high = value.charCodeAt(index)
  if (high < 0xd800 || high > 0xdbff || index + 1 >= value.length) return false
  const low = value.charCodeAt(index + 1)
  return low >= 0xdc00 && low <= 0xdfff
}
