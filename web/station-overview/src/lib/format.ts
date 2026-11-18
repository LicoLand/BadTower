/**
 * Deterministic presentation formatting. Locale is pinned so output is stable
 * across operator machines; inputs are numbers only, never raw strings.
 */

const countFormat = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 })
const byteFormat = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 })

const BYTE_UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB'] as const

export function formatCount(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return '0'
  return countFormat.format(Math.floor(value))
}

export function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return '0 B'
  let amount = value
  let unit = 0
  while (amount >= 1024 && unit < BYTE_UNITS.length - 1) {
    amount /= 1024
    unit += 1
  }
  const formatted = unit === 0 ? String(Math.round(amount)) : byteFormat.format(amount)
  return `${formatted} ${BYTE_UNITS[unit]}`
}

/** Format one 0–1 ratio as a clamped integer percent. */
export function formatPercent(ratio: number): string {
  if (!Number.isFinite(ratio) || ratio <= 0) return '0%'
  if (ratio >= 1) return '100%'
  return `${Math.round(ratio * 100)}%`
}

/** Coarse relative age for the freshness label; no absolute timestamps. */
export function formatRelativeAge(ageMs: number): string {
  if (!Number.isFinite(ageMs) || ageMs < 0) return 'just now'
  const seconds = Math.floor(ageMs / 1000)
  if (seconds < 5) return 'just now'
  if (seconds < 60) return `${seconds} s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes} min ago`
  const hours = Math.floor(minutes / 60)
  return `${hours} h ago`
}
