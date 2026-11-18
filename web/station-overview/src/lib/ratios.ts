/**
 * Presentation-only capacity ratios, per the frozen design: effective
 * ceilings derive from per-namespace ceilings times the configured namespace
 * count; ratios are zero-safe and clamped to 0–1. No other arithmetic on
 * payload numbers is permitted in the UI.
 */

import type { OverviewCapacity, ResourceVector } from '../api/overview'

export type ResourceKey = keyof ResourceVector

export const RESOURCE_KEYS = [
  'addresses',
  'transportUnits',
  'logicalRetainedBytes',
  'activeClaims',
] as const satisfies readonly ResourceKey[]

export type SaturationBand = 'normal' | 'elevated' | 'saturated'

export function clampRatio(value: number): number {
  if (!Number.isFinite(value) || value <= 0) return 0
  return value >= 1 ? 1 : value
}

export function effectiveCeiling(capacity: OverviewCapacity, key: ResourceKey): number {
  return capacity.implementationCeiling.perNamespace[key] * capacity.configuredNamespaceCount
}

export function usageRatio(capacity: OverviewCapacity, key: ResourceKey): number {
  const ceiling = effectiveCeiling(capacity, key)
  if (ceiling <= 0) return 0
  return clampRatio(capacity.aggregateUsage[key] / ceiling)
}

export function namespaceRatio(capacity: OverviewCapacity): number {
  const ceiling = capacity.implementationCeiling.namespaceCount
  if (ceiling <= 0) return 0
  return clampRatio(capacity.configuredNamespaceCount / ceiling)
}

export function saturationBand(ratio: number): SaturationBand {
  const clamped = clampRatio(ratio)
  if (clamped > 0.9) return 'saturated'
  if (clamped >= 0.7) return 'elevated'
  return 'normal'
}
