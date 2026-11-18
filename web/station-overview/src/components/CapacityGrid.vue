<script setup lang="ts">
import { computed } from 'vue'

import type { OverviewCapacity } from '../api/overview'
import type { ResourceKey, SaturationBand } from '../lib/ratios'
import {
  effectiveCeiling,
  namespaceRatio,
  RESOURCE_KEYS,
  saturationBand,
  usageRatio,
} from '../lib/ratios'
import { formatBytes, formatCount } from '../lib/format'
import ResourceMeter from './ResourceMeter.vue'

const props = defineProps<{
  capacity: OverviewCapacity
}>()

// Fixed UI copy keyed by the strict payload field names — payload numbers are
// only ever rendered through the format helpers, never as raw strings.
const RESOURCE_LABELS: Record<ResourceKey, string> = {
  addresses: 'Addresses',
  transportUnits: 'Transport units',
  logicalRetainedBytes: 'Retained bytes',
  activeClaims: 'Active claims',
}

const BAND_LABELS: Record<SaturationBand, string> = {
  normal: 'Normal',
  elevated: 'Elevated',
  saturated: 'Saturated',
}

interface MeterModel {
  key: string
  label: string
  usage: string
  ceiling: string
  ratio: number
  band: string
  ceilingZero: boolean
}

const namespaceMeter = computed<MeterModel>(() => {
  const ceiling = props.capacity.implementationCeiling.namespaceCount
  const ratio = namespaceRatio(props.capacity)
  return {
    key: 'namespaces',
    label: 'Namespaces',
    usage: formatCount(props.capacity.configuredNamespaceCount),
    ceiling: formatCount(ceiling),
    ratio,
    band: BAND_LABELS[saturationBand(ratio)],
    ceilingZero: ceiling <= 0,
  }
})

const resourceMeters = computed<MeterModel[]>(() =>
  RESOURCE_KEYS.map((key) => {
    const ceiling = effectiveCeiling(props.capacity, key)
    const ratio = usageRatio(props.capacity, key)
    const format = key === 'logicalRetainedBytes' ? formatBytes : formatCount
    return {
      key,
      label: RESOURCE_LABELS[key],
      usage: format(props.capacity.aggregateUsage[key]),
      ceiling: format(ceiling),
      ratio,
      band: BAND_LABELS[saturationBand(ratio)],
      ceilingZero: ceiling <= 0,
    }
  }),
)
</script>

<template>
  <section class="capacity-grid" aria-labelledby="capacity-grid-heading">
    <h2 id="capacity-grid-heading" class="capacity-grid__heading">Capacity</h2>
    <ul class="capacity-grid__meters">
      <li class="capacity-grid__meter">
        <ResourceMeter
          :label="namespaceMeter.label"
          :usage="namespaceMeter.usage"
          :ceiling="namespaceMeter.ceiling"
          :ratio="namespaceMeter.ratio"
          :band="namespaceMeter.band"
          :ceiling-zero="namespaceMeter.ceilingZero"
        />
      </li>
      <li v-for="meter in resourceMeters" :key="meter.key" class="capacity-grid__meter">
        <ResourceMeter
          :label="meter.label"
          :usage="meter.usage"
          :ceiling="meter.ceiling"
          :ratio="meter.ratio"
          :band="meter.band"
          :ceiling-zero="meter.ceilingZero"
        />
      </li>
    </ul>
  </section>
</template>
