<script setup lang="ts">
import { computed } from 'vue'

const props = withDefaults(
  defineProps<{
    label: string
    /** Pre-formatted usage figure, e.g. "1.2 GiB". */
    usage: string
    /** Pre-formatted ceiling figure, e.g. "4 GiB". */
    ceiling: string
    /** Utilization ratio in the inclusive range 0..1. */
    ratio: number
    /** Utilization band name, conveyed as text (not color). */
    band: string
    /** True when the resource has no configured ceiling; ratio is then meaningless. */
    ceilingZero?: boolean
  }>(),
  {
    ceilingZero: false,
  },
)

const clampedRatio = computed<number>(() => Math.min(1, Math.max(0, props.ratio)))

const percent = computed<number>(() => Math.round(clampedRatio.value * 100))

const valueText = computed<string>(() =>
  props.ceilingZero
    ? `${props.usage} used; no ceiling configured`
    : `${props.usage} of ${props.ceiling} used, ${percent.value} percent`,
)
</script>

<template>
  <figure class="resource-meter">
    <figcaption class="resource-meter__header">
      <span class="resource-meter__label">{{ props.label }}</span>
      <span class="resource-meter__band">{{ props.band }}</span>
    </figcaption>
    <meter
      class="resource-meter__gauge"
      min="0"
      max="1"
      :value="props.ceilingZero ? 0 : clampedRatio"
      :aria-label="props.label"
      :aria-valuetext="valueText"
    >
      {{ valueText }}
    </meter>
    <p class="resource-meter__values">
      <span class="resource-meter__usage">{{ props.usage }}</span>
      <span v-if="props.ceilingZero" class="resource-meter__ceiling">No ceiling configured</span>
      <span v-else class="resource-meter__ceiling">of {{ props.ceiling }}</span>
    </p>
  </figure>
</template>
