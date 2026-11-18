<script setup lang="ts">
import { computed } from 'vue'

export type StatusTone = 'ok' | 'elevated' | 'saturated' | 'neutral'

const props = defineProps<{
  label: string
  tone: StatusTone
}>()

// Tone is always paired with explicit text so meaning never depends on color alone.
const toneText: Record<StatusTone, string> = {
  ok: 'OK',
  elevated: 'Elevated',
  saturated: 'Saturated',
  neutral: 'Unknown',
}

const toneLabel = computed<string>(() => toneText[props.tone])
</script>

<template>
  <span class="status-badge" :class="`status-badge--${props.tone}`" role="status">
    <span class="status-badge__label">{{ props.label }}</span>
    <span class="status-badge__tone" aria-hidden="false">{{ toneLabel }}</span>
  </span>
</template>
