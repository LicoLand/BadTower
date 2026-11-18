/**
 * Framework-light in-memory polling store for the Station Overview.
 *
 * Single-flight `setTimeout` chain (never `setInterval`), pollAfterMs floor,
 * bounded retryAfterMs and exponential backoff, local-receipt freshness and
 * staleness, visibility-agnostic core (the shell wires visibility to
 * stop/refresh), and clean stop via generation guard plus abort. The 401/403
 * path wipes the session credential. Nothing here persists anything.
 */

import { fetchOverview, type FetchOutcome } from '../api/client'
import type { OperatorSession } from '../api/operatorSession'
import type { Overview } from '../api/overview'

export const MIN_POLL_MS = 1000
export const MAX_POLL_MS = 60000
export const DEFAULT_POLL_MS = 5000
export const STALE_MIN_MS = 15000
export const STALE_MULTIPLIER = 3

const BACKOFF_STEPS_MS = [5000, 10000, 20000, 40000, 60000] as const

export type OverviewPhase = 'credential' | 'loading' | 'ready' | 'unavailable'

export type FetchOnce = (signal: AbortSignal) => Promise<FetchOutcome>

export interface OverviewStoreDeps {
  session: OperatorSession
  fetchOnce?: FetchOnce
  now?: () => number
}

export interface OverviewStoreState {
  phase: OverviewPhase
  failures: number
  intervalMs: number
  receivedAtMs: number | null
  hasSnapshot: boolean
  /** True when the loop halted (auth wipe or non-retryable error); manual retry restarts it. */
  halted: boolean
}

export interface OverviewStore {
  /** Begin polling. Without a credential the store sits in the credential phase. */
  start(): void
  /** Halt the loop and abort any in-flight request (teardown, hidden tab, sign-out). */
  stop(): void
  /** Manual immediate attempt; no-op while a request is in flight. Restarts a halted loop. */
  refresh(): void
  /** Wipe all state back to the credential phase (sign-out). */
  reset(): void
  isStale(atMs?: number): boolean
  isPolling(): boolean
  isInFlight(): boolean
  getOverview(): Overview | null
  getState(): OverviewStoreState
  subscribe(listener: () => void): () => void
}

export function createOverviewStore(deps: OverviewStoreDeps): OverviewStore {
  const fetchOnce: FetchOnce =
    deps.fetchOnce ?? ((signal) => fetchOverview({ session: deps.session, signal }))
  const now = deps.now ?? (() => Date.now())

  let phase: OverviewPhase = 'credential'
  let overview: Overview | null = null
  let receivedAtMs: number | null = null
  let failures = 0
  let intervalMs = DEFAULT_POLL_MS
  let halted = false
  let timer: ReturnType<typeof setTimeout> | null = null
  let inFlight = false
  let generation = 0
  let activeAbort: AbortController | null = null
  const listeners = new Set<() => void>()

  const emit = (): void => {
    for (const listener of listeners) listener()
  }

  const clampPoll = (ms: number): number =>
    Math.min(MAX_POLL_MS, Math.max(MIN_POLL_MS, Math.trunc(ms)))

  const backoffMs = (): number =>
    BACKOFF_STEPS_MS[Math.min(Math.max(failures - 1, 0), BACKOFF_STEPS_MS.length - 1)]

  function clearTimer(): void {
    if (timer !== null) {
      clearTimeout(timer)
      timer = null
    }
  }

  function schedule(delayMs: number, gen: number): void {
    clearTimer()
    timer = setTimeout(() => {
      timer = null
      void attempt(gen)
    }, delayMs)
  }

  async function attempt(gen: number): Promise<void> {
    if (gen !== generation || inFlight) return
    inFlight = true
    const controller = new AbortController()
    activeAbort = controller
    try {
      const outcome = await fetchOnce(controller.signal)
      if (gen !== generation) return
      handleOutcome(outcome, gen)
    } catch {
      // fetchOnce is contractually total; stay fail-closed if it ever throws.
      if (gen === generation) handleOutcome({ kind: 'failed' }, gen)
    } finally {
      if (activeAbort === controller) activeAbort = null
      inFlight = false
    }
  }

  function handleOutcome(outcome: FetchOutcome, gen: number): void {
    switch (outcome.kind) {
      case 'ok': {
        overview = outcome.overview
        receivedAtMs = now()
        failures = 0
        halted = false
        intervalMs = clampPoll(outcome.overview.pollAfterMs)
        phase = 'ready'
        schedule(intervalMs, gen)
        break
      }
      case 'auth': {
        deps.session.clear()
        failures = 0
        halted = true
        phase = 'credential'
        clearTimer()
        break
      }
      case 'retryable': {
        failures += 1
        phase = overview === null ? 'loading' : 'ready'
        const delay = outcome.retryAfterMs === null ? backoffMs() : clampPoll(outcome.retryAfterMs)
        schedule(delay, gen)
        break
      }
      case 'fatal': {
        halted = true
        phase = overview === null ? 'unavailable' : 'ready'
        clearTimer()
        break
      }
      case 'failed': {
        failures += 1
        phase = overview === null ? 'unavailable' : 'ready'
        schedule(backoffMs(), gen)
        break
      }
    }
    emit()
  }

  return {
    start() {
      if (!deps.session.hasCredential()) {
        phase = 'credential'
        emit()
        return
      }
      generation += 1
      clearTimer()
      halted = false
      phase = overview === null ? 'loading' : 'ready'
      emit()
      void attempt(generation)
    },
    stop() {
      generation += 1
      clearTimer()
      activeAbort?.abort()
    },
    refresh() {
      if (inFlight) return
      if (!deps.session.hasCredential()) {
        phase = 'credential'
        emit()
        return
      }
      clearTimer()
      halted = false
      void attempt(generation)
    },
    reset() {
      generation += 1
      clearTimer()
      activeAbort?.abort()
      deps.session.clear()
      overview = null
      receivedAtMs = null
      failures = 0
      halted = false
      intervalMs = DEFAULT_POLL_MS
      phase = 'credential'
      emit()
    },
    isStale(atMs?: number) {
      if (receivedAtMs === null) return false
      const at = atMs ?? now()
      return at - receivedAtMs > Math.max(STALE_MULTIPLIER * intervalMs, STALE_MIN_MS)
    },
    isPolling() {
      return timer !== null
    },
    isInFlight() {
      return inFlight
    },
    getOverview() {
      return overview
    },
    getState() {
      return { phase, failures, intervalMs, receivedAtMs, hasSnapshot: overview !== null, halted }
    },
    subscribe(listener: () => void) {
      listeners.add(listener)
      return () => {
        listeners.delete(listener)
      }
    },
  }
}
