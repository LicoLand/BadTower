/**
 * Fixed, exhaustive label and tone maps for every closed backend enum, plus
 * bounded error-reason mapping with a generic fallback. Raw server strings
 * are never rendered: enum tokens index these maps and unknown error reasons
 * collapse to generic copy.
 */

import type { Lifecycle, Pressure, ReadinessReason, ReadinessStatus } from '../api/overview'

export type Tone = 'ok' | 'elevated' | 'saturated' | 'neutral'

export const LIFECYCLE_LABELS: Record<Lifecycle, string> = {
  STARTING: 'Starting',
  RUNNING: 'Running',
  DRAINING: 'Draining',
  STOPPED: 'Stopped',
}

export const LIFECYCLE_TONES: Record<Lifecycle, Tone> = {
  STARTING: 'elevated',
  RUNNING: 'ok',
  DRAINING: 'elevated',
  STOPPED: 'neutral',
}

export const READINESS_STATUS_LABELS: Record<ReadinessStatus, string> = {
  SERVING: 'Serving',
  NOT_SERVING: 'Not serving',
}

export const READINESS_STATUS_TONES: Record<ReadinessStatus, Tone> = {
  SERVING: 'ok',
  NOT_SERVING: 'elevated',
}

export const READINESS_REASON_LABELS: Record<Exclude<ReadinessReason, 'NONE'>, string> = {
  STARTING: 'Starting up',
  CLOCK_UNSAFE: 'Clock unsafe',
  DRAINING: 'Draining',
  STOPPED: 'Stopped',
}

/** Reason label shown only while NOT_SERVING; NONE renders no reason line. */
export function readinessReasonLabel(reason: ReadinessReason): string | null {
  return reason === 'NONE' ? null : READINESS_REASON_LABELS[reason]
}

export const PRESSURE_LABELS: Record<Pressure, string> = {
  NORMAL: 'Normal',
  SOFT: 'Elevated',
  HARD: 'Saturated',
}

export const PRESSURE_TONES: Record<Pressure, Tone> = {
  NORMAL: 'ok',
  SOFT: 'elevated',
  HARD: 'saturated',
}

/** Fixed copy for phases and empty states; never server-derived. */
export const PHASE_LABELS = {
  loading: 'Contacting the station…',
  stale: 'Showing last known state — retrying',
  unavailable: 'Station unavailable',
  unavailableDetail: 'The station cannot be reached right now. Automatic retries continue in the background.',
  credentialRequired: 'Operator credential required',
  credentialRejected: 'Credential rejected or expired.',
  emptyNamespaces: 'No namespaces configured',
  emptyUsage: 'No traffic retained',
} as const

const GENERIC_ERROR_LABEL = 'The station reported a temporary problem. Retrying automatically.'

/**
 * Map one bounded error-reason token to fixed generic copy. The raw reason is
 * never rendered; unknown tokens fall back to the same generic sentence.
 */
export function overviewErrorLabel(reason: string): string {
  switch (reason) {
    case 'CAPACITY_EXCEEDED':
    case 'RATE_LIMITED':
    case 'DRAINING':
    case 'OVERLOADED':
      return 'The station is limiting new work. Retrying automatically.'
    case 'STORAGE_UNAVAILABLE':
    case 'CLOCK_UNSAFE':
    case 'UNAVAILABLE':
    case 'INTERNAL':
      return 'The station is temporarily unavailable. Retrying automatically.'
    default:
      return GENERIC_ERROR_LABEL
  }
}
