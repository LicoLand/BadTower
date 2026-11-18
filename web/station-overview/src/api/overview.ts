/**
 * Strict runtime contract for `GET /operator/v1/overview`.
 *
 * The panel renders only what these parsers accept: closed enums, non-negative
 * aggregate integers, and nothing else. Unknown or extra fields, out-of-set
 * tokens, invalid readiness pairings, identifiers, timestamps, raw strings, or
 * nested rows cause the whole payload to be rejected — never a partial render.
 */

export const OVERVIEW_SCHEMA = 'badtower.station-overview.v1'
export const OVERVIEW_ERROR_SCHEMA = 'badtower.station-overview.error.v1'

export const LIFECYCLES = ['STARTING', 'RUNNING', 'DRAINING', 'STOPPED'] as const
export type Lifecycle = (typeof LIFECYCLES)[number]

export const READINESS_STATUSES = ['SERVING', 'NOT_SERVING'] as const
export type ReadinessStatus = (typeof READINESS_STATUSES)[number]

export const READINESS_REASONS = ['NONE', 'STARTING', 'CLOCK_UNSAFE', 'DRAINING', 'STOPPED'] as const
export type ReadinessReason = (typeof READINESS_REASONS)[number]

export const PRESSURES = ['NORMAL', 'SOFT', 'HARD'] as const
export type Pressure = (typeof PRESSURES)[number]

export interface ResourceVector {
  addresses: number
  transportUnits: number
  logicalRetainedBytes: number
  activeClaims: number
}

export interface Readiness {
  status: ReadinessStatus
  reason: ReadinessReason
}

export interface OverviewCapacity {
  configuredNamespaceCount: number
  implementationCeiling: {
    namespaceCount: number
    perNamespace: ResourceVector
  }
  aggregateUsage: ResourceVector
}

export interface Overview {
  schema: typeof OVERVIEW_SCHEMA
  lifecycle: Lifecycle
  readiness: Readiness
  pressure: Pressure
  capacity: OverviewCapacity
  pollAfterMs: number
}

export interface OverviewError {
  schema: typeof OVERVIEW_ERROR_SCHEMA
  reason: string
  retryable: boolean
  retryAfterMs: number
}

const OVERVIEW_KEYS = ['schema', 'lifecycle', 'readiness', 'pressure', 'capacity', 'pollAfterMs'] as const
const READINESS_KEYS = ['status', 'reason'] as const
const CAPACITY_KEYS = ['configuredNamespaceCount', 'implementationCeiling', 'aggregateUsage'] as const
const CEILING_KEYS = ['namespaceCount', 'perNamespace'] as const
const RESOURCE_VECTOR_KEYS = ['addresses', 'transportUnits', 'logicalRetainedBytes', 'activeClaims'] as const
const ERROR_KEYS = ['schema', 'reason', 'retryable', 'retryAfterMs'] as const

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function hasExactKeys(record: Record<string, unknown>, keys: readonly string[]): boolean {
  return (
    Object.keys(record).length === keys.length &&
    keys.every((key) => Object.prototype.hasOwnProperty.call(record, key))
  )
}

function isNonNegativeInt(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}

function parseResourceVector(value: unknown): ResourceVector | null {
  if (!isRecord(value) || !hasExactKeys(value, RESOURCE_VECTOR_KEYS)) return null
  const vector = {
    addresses: value.addresses,
    transportUnits: value.transportUnits,
    logicalRetainedBytes: value.logicalRetainedBytes,
    activeClaims: value.activeClaims,
  }
  if (!Object.values(vector).every(isNonNegativeInt)) return null
  return vector as ResourceVector
}

function parseReadiness(value: unknown): Readiness | null {
  if (!isRecord(value) || !hasExactKeys(value, READINESS_KEYS)) return null
  const { status, reason } = value
  if (!READINESS_STATUSES.includes(status as ReadinessStatus)) return null
  if (!READINESS_REASONS.includes(reason as ReadinessReason)) return null
  // Amendment A1 pairing invariant: SERVING pairs only with NONE;
  // NOT_SERVING pairs only with a non-NONE reason. Any other combination
  // rejects the whole payload.
  if (status === 'SERVING' && reason !== 'NONE') return null
  if (status === 'NOT_SERVING' && reason === 'NONE') return null
  return { status: status as ReadinessStatus, reason: reason as ReadinessReason }
}

function parseCapacity(value: unknown): OverviewCapacity | null {
  if (!isRecord(value) || !hasExactKeys(value, CAPACITY_KEYS)) return null
  if (!isNonNegativeInt(value.configuredNamespaceCount)) return null
  const ceiling = value.implementationCeiling
  if (!isRecord(ceiling) || !hasExactKeys(ceiling, CEILING_KEYS)) return null
  if (!isNonNegativeInt(ceiling.namespaceCount)) return null
  const perNamespace = parseResourceVector(ceiling.perNamespace)
  if (perNamespace === null) return null
  const aggregateUsage = parseResourceVector(value.aggregateUsage)
  if (aggregateUsage === null) return null
  return {
    configuredNamespaceCount: value.configuredNamespaceCount,
    implementationCeiling: { namespaceCount: ceiling.namespaceCount, perNamespace },
    aggregateUsage,
  }
}

/** Parse one success payload; returns null on any contract violation. */
export function parseOverview(value: unknown): Overview | null {
  if (!isRecord(value) || !hasExactKeys(value, OVERVIEW_KEYS)) return null
  if (value.schema !== OVERVIEW_SCHEMA) return null
  if (!LIFECYCLES.includes(value.lifecycle as Lifecycle)) return null
  if (!PRESSURES.includes(value.pressure as Pressure)) return null
  const readiness = parseReadiness(value.readiness)
  if (readiness === null) return null
  const capacity = parseCapacity(value.capacity)
  if (capacity === null) return null
  if (!isNonNegativeInt(value.pollAfterMs)) return null
  return {
    schema: OVERVIEW_SCHEMA,
    lifecycle: value.lifecycle as Lifecycle,
    readiness,
    pressure: value.pressure as Pressure,
    capacity,
    pollAfterMs: value.pollAfterMs,
  }
}

/**
 * Parse one error payload (`badtower.station-overview.error.v1`). `reason` is
 * accepted as a bounded opaque token and must only ever index a fixed label
 * map — it is never rendered raw.
 */
export function parseOverviewError(value: unknown): OverviewError | null {
  if (!isRecord(value) || !hasExactKeys(value, ERROR_KEYS)) return null
  if (value.schema !== OVERVIEW_ERROR_SCHEMA) return null
  if (typeof value.reason !== 'string' || value.reason.length === 0 || value.reason.length > 64) return null
  if (typeof value.retryable !== 'boolean') return null
  if (!isNonNegativeInt(value.retryAfterMs)) return null
  return {
    schema: OVERVIEW_ERROR_SCHEMA,
    reason: value.reason,
    retryable: value.retryable,
    retryAfterMs: value.retryAfterMs,
  }
}
