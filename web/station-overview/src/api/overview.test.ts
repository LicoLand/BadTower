import { describe, expect, it } from 'vitest'

import canonicalFixture from '../../contracts/overview.fixture.json'
import drainingFixture from '../../contracts/overview.draining.fixture.json'
import startingFixture from '../../contracts/overview.starting.fixture.json'
import errorFixture from '../../contracts/overview-error.fixture.json'
import {
  OVERVIEW_ERROR_SCHEMA,
  OVERVIEW_SCHEMA,
  parseOverview,
  parseOverviewError,
} from './overview'

function clone<T>(value: T): T {
  return structuredClone(value)
}

describe('parseOverview', () => {
  it('accepts the canonical fixture and preserves values', () => {
    const overview = parseOverview(clone(canonicalFixture))
    expect(overview).not.toBeNull()
    expect(overview?.schema).toBe(OVERVIEW_SCHEMA)
    expect(overview?.lifecycle).toBe('RUNNING')
    expect(overview?.readiness).toEqual({ status: 'SERVING', reason: 'NONE' })
    expect(overview?.pressure).toBe('NORMAL')
    expect(overview?.capacity.configuredNamespaceCount).toBe(3)
    expect(overview?.capacity.aggregateUsage.transportUnits).toBe(4096)
    expect(overview?.pollAfterMs).toBe(5000)
  })

  it('accepts the draining edge fixture (NOT_SERVING + DRAINING)', () => {
    const overview = parseOverview(clone(drainingFixture))
    expect(overview?.readiness).toEqual({ status: 'NOT_SERVING', reason: 'DRAINING' })
    expect(overview?.lifecycle).toBe('DRAINING')
  })

  it('accepts the starting edge fixture (NOT_SERVING + STARTING, zero usage)', () => {
    const overview = parseOverview(clone(startingFixture))
    expect(overview?.readiness).toEqual({ status: 'NOT_SERVING', reason: 'STARTING' })
    expect(overview?.capacity.configuredNamespaceCount).toBe(0)
  })

  it.each(['STARTING', 'CLOCK_UNSAFE', 'STOPPED'] as const)(
    'accepts NOT_SERVING with non-NONE reason %s',
    (reason) => {
      const payload = clone(canonicalFixture)
      payload.readiness = { status: 'NOT_SERVING', reason }
      expect(parseOverview(payload)?.readiness).toEqual({ status: 'NOT_SERVING', reason })
    },
  )

  it('rejects an unknown top-level field', () => {
    const payload = { ...clone(canonicalFixture), stationName: 'tower-1' }
    expect(parseOverview(payload)).toBeNull()
  })

  it('rejects unknown nested fields at every level', () => {
    const withReadinessExtra = clone(canonicalFixture)
    Object.assign(withReadinessExtra.readiness, { detail: 'raw text' })
    expect(parseOverview(withReadinessExtra)).toBeNull()

    const withCapacityExtra = clone(canonicalFixture)
    Object.assign(withCapacityExtra.capacity, { namespaces: [] })
    expect(parseOverview(withCapacityExtra)).toBeNull()

    const withVectorExtra = clone(canonicalFixture)
    Object.assign(withVectorExtra.capacity.implementationCeiling.perNamespace, { connections: 3 })
    expect(parseOverview(withVectorExtra)).toBeNull()
  })

  it('rejects omitted fields', () => {
    const payload = clone(canonicalFixture) as Record<string, unknown>
    delete payload.pollAfterMs
    expect(parseOverview(payload)).toBeNull()
  })

  it('rejects legacy READY / NOT_READY readiness tokens', () => {
    const ready = clone(canonicalFixture)
    ready.readiness = { status: 'READY', reason: 'NONE' } as never
    expect(parseOverview(ready)).toBeNull()

    const notReady = clone(canonicalFixture)
    notReady.readiness = { status: 'NOT_READY', reason: 'DRAINING' } as never
    expect(parseOverview(notReady)).toBeNull()
  })

  it('rejects SERVING paired with a non-NONE reason', () => {
    const payload = clone(canonicalFixture)
    payload.readiness = { status: 'SERVING', reason: 'DRAINING' }
    expect(parseOverview(payload)).toBeNull()
  })

  it('rejects NOT_SERVING paired with NONE', () => {
    const payload = clone(canonicalFixture)
    payload.readiness = { status: 'NOT_SERVING', reason: 'NONE' }
    expect(parseOverview(payload)).toBeNull()
  })

  it('rejects a wrong schema value', () => {
    const payload = clone(canonicalFixture)
    payload.schema = 'badtower.station-overview.v0'
    expect(parseOverview(payload)).toBeNull()
  })

  it('rejects unknown lifecycle and pressure tokens', () => {
    const badLifecycle = clone(canonicalFixture)
    badLifecycle.lifecycle = 'RECOVERING' as never
    expect(parseOverview(badLifecycle)).toBeNull()

    const badPressure = clone(canonicalFixture)
    badPressure.pressure = 'CRITICAL' as never
    expect(parseOverview(badPressure)).toBeNull()
  })

  it('rejects negative, fractional, and string-typed numerics', () => {
    const negative = clone(canonicalFixture)
    negative.capacity.aggregateUsage.addresses = -1
    expect(parseOverview(negative)).toBeNull()

    const fractional = clone(canonicalFixture)
    fractional.capacity.configuredNamespaceCount = 1.5
    expect(parseOverview(fractional)).toBeNull()

    const stringy = clone(canonicalFixture)
    Object.assign(stringy.capacity.aggregateUsage, { transportUnits: '4096' })
    expect(parseOverview(stringy)).toBeNull()

    const negativePoll = clone(canonicalFixture)
    negativePoll.pollAfterMs = -100
    expect(parseOverview(negativePoll)).toBeNull()
  })

  it('rejects non-object inputs', () => {
    expect(parseOverview(null)).toBeNull()
    expect(parseOverview('RUNNING')).toBeNull()
    expect(parseOverview([clone(canonicalFixture)])).toBeNull()
    expect(parseOverview(42)).toBeNull()
  })
})

describe('parseOverviewError', () => {
  it('accepts the error fixture', () => {
    const failure = parseOverviewError(clone(errorFixture))
    expect(failure).toEqual({
      schema: OVERVIEW_ERROR_SCHEMA,
      reason: 'CAPACITY_EXCEEDED',
      retryable: true,
      retryAfterMs: 15000,
    })
  })

  it('rejects extra fields and a wrong schema', () => {
    expect(parseOverviewError({ ...clone(errorFixture), message: 'raw text' })).toBeNull()
    const wrongSchema = { ...clone(errorFixture), schema: 'badtower.station-overview.v1' }
    expect(parseOverviewError(wrongSchema)).toBeNull()
  })

  it('rejects non-string reason, empty reason, non-boolean retryable, negative retryAfterMs', () => {
    expect(parseOverviewError({ ...clone(errorFixture), reason: 7 })).toBeNull()
    expect(parseOverviewError({ ...clone(errorFixture), reason: '' })).toBeNull()
    expect(parseOverviewError({ ...clone(errorFixture), retryable: 'yes' })).toBeNull()
    expect(parseOverviewError({ ...clone(errorFixture), retryAfterMs: -5 })).toBeNull()
  })
})
