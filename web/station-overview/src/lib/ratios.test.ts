import { describe, expect, it } from 'vitest'

import canonicalFixture from '../../contracts/overview.fixture.json'
import startingFixture from '../../contracts/overview.starting.fixture.json'
import type { OverviewCapacity, ResourceVector } from '../api/overview'
import { parseOverview } from '../api/overview'
import {
  effectiveCeiling,
  namespaceRatio,
  RESOURCE_KEYS,
  saturationBand,
  usageRatio,
} from './ratios'

const canonical = parseOverview(structuredClone(canonicalFixture))
if (canonical === null) throw new Error('canonical fixture must parse')
const starting = parseOverview(structuredClone(startingFixture))
if (starting === null) throw new Error('starting fixture must parse')

function syntheticCapacity(overrides: {
  configured?: number
  ceilingNamespaces?: number
  perNamespace?: Partial<ResourceVector>
  usage?: Partial<ResourceVector>
}): OverviewCapacity {
  const zero: ResourceVector = { addresses: 0, transportUnits: 0, logicalRetainedBytes: 0, activeClaims: 0 }
  return {
    configuredNamespaceCount: overrides.configured ?? 0,
    implementationCeiling: {
      namespaceCount: overrides.ceilingNamespaces ?? 0,
      perNamespace: { ...zero, ...(overrides.perNamespace ?? {}) },
    },
    aggregateUsage: { ...zero, ...(overrides.usage ?? {}) },
  }
}

describe('effectiveCeiling and usageRatio', () => {
  it('derives ceilings from per-namespace ceilings times configured namespaces', () => {
    const capacity = canonical.capacity
    expect(effectiveCeiling(capacity, 'addresses')).toBe(30000)
    expect(effectiveCeiling(capacity, 'transportUnits')).toBe(300000)
    expect(effectiveCeiling(capacity, 'activeClaims')).toBe(30000)
  })

  it('computes ratios from aggregate usage over the effective ceiling', () => {
    const capacity = canonical.capacity
    expect(usageRatio(capacity, 'addresses')).toBeCloseTo(128 / 30000, 6)
    expect(usageRatio(capacity, 'transportUnits')).toBeCloseTo(4096 / 300000, 6)
    expect(namespaceRatio(capacity)).toBeCloseTo(3 / 1000, 6)
  })

  it('is zero-safe when no namespaces are configured', () => {
    const capacity = starting.capacity
    expect(capacity.configuredNamespaceCount).toBe(0)
    for (const key of RESOURCE_KEYS) {
      expect(usageRatio(capacity, key), key).toBe(0)
    }
  })

  it('is zero-safe when ceilings are zero (no NaN or Infinity)', () => {
    const capacity = syntheticCapacity({
      configured: 2,
      ceilingNamespaces: 0,
      usage: { addresses: 50 },
    })
    expect(usageRatio(capacity, 'addresses')).toBe(0)
    expect(namespaceRatio(capacity)).toBe(0)
  })

  it('clamps ratios at 1 when usage exceeds the ceiling', () => {
    const capacity = syntheticCapacity({
      configured: 1,
      ceilingNamespaces: 10,
      perNamespace: { addresses: 10 },
      usage: { addresses: 25 },
    })
    expect(usageRatio(capacity, 'addresses')).toBe(1)
    expect(namespaceRatio(syntheticCapacity({ configured: 1500, ceilingNamespaces: 1000 }))).toBe(1)
  })
})

describe('saturationBand', () => {
  it('bands ratios at the frozen thresholds', () => {
    expect(saturationBand(0)).toBe('normal')
    expect(saturationBand(0.69)).toBe('normal')
    expect(saturationBand(0.7)).toBe('elevated')
    expect(saturationBand(0.9)).toBe('elevated')
    expect(saturationBand(0.91)).toBe('saturated')
    expect(saturationBand(1)).toBe('saturated')
  })

  it('treats invalid input as normal', () => {
    expect(saturationBand(Number.NaN)).toBe('normal')
    expect(saturationBand(-1)).toBe('normal')
  })
})
