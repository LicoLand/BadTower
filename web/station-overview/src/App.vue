<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, shallowRef } from 'vue'

import { createOperatorSession } from './api/operatorSession'
import type { Overview } from './api/overview'
import { formatRelativeAge } from './lib/format'
import {
  LIFECYCLE_LABELS,
  LIFECYCLE_TONES,
  PHASE_LABELS,
  PRESSURE_LABELS,
  PRESSURE_TONES,
  READINESS_STATUS_LABELS,
  READINESS_STATUS_TONES,
  readinessReasonLabel,
} from './lib/labels'
import { createOverviewStore, type OverviewStoreState } from './store/overview'
import CapacityGrid from './components/CapacityGrid.vue'
import CredentialGate from './components/CredentialGate.vue'
import StatusBadge from './components/StatusBadge.vue'

// One memory-only operator session and one polling store for this page
// lifetime. The credential never leaves the session closure: it is never
// rendered, persisted, logged, or placed in the URL.
const session = createOperatorSession()
const store = createOverviewStore({ session })

// Bridge the store's subscribe/emit contract into Vue reactivity.
const storeState = ref<OverviewStoreState>(store.getState())
const overview = shallowRef<Overview | null>(store.getOverview())
const unsubscribe = store.subscribe(() => {
  storeState.value = store.getState()
  overview.value = store.getOverview()
})

// Local clock driving the relative freshness label; paused while the tab is
// hidden. Purely presentational — it never triggers network activity.
const nowMs = ref<number>(Date.now())
let clock: ReturnType<typeof setInterval> | null = null

function startClock(): void {
  if (clock === null) {
    clock = setInterval(() => {
      nowMs.value = Date.now()
    }, 1000)
  }
}

function stopClock(): void {
  if (clock !== null) {
    clearInterval(clock)
    clock = null
  }
}

// True once the operator has submitted a credential this page lifetime, so a
// later drop back to the credential phase reads as a rejection, not first run.
const submitted = ref(false)

function handleCredentialSubmit(value: string): void {
  // CredentialGate has already cleared its own input; the value now lives
  // only inside the session closure. Starting the store begins polling.
  session.setCredential(value)
  submitted.value = true
  store.start()
}

function handleSignOut(): void {
  submitted.value = false
  store.reset()
}

const phase = computed(() => storeState.value.phase)
const hasSnapshot = computed(() => storeState.value.hasSnapshot)
const credentialRejected = computed(() => submitted.value && phase.value === 'credential')

const stale = computed(
  () => storeState.value.receivedAtMs !== null && store.isStale(nowMs.value),
)

const freshnessText = computed<string | null>(() => {
  const receivedAtMs = storeState.value.receivedAtMs
  if (receivedAtMs === null) return null
  return formatRelativeAge(nowMs.value - receivedAtMs)
})

const pollSeconds = computed<number>(() => Math.max(1, Math.round(storeState.value.intervalMs / 1000)))

const lifecycleLabel = computed(() =>
  overview.value === null ? '' : LIFECYCLE_LABELS[overview.value.lifecycle],
)
const lifecycleTone = computed(() =>
  overview.value === null ? 'neutral' : LIFECYCLE_TONES[overview.value.lifecycle],
)
const readinessLabel = computed(() => {
  const current = overview.value
  if (current === null) return ''
  const status = READINESS_STATUS_LABELS[current.readiness.status]
  const reason = readinessReasonLabel(current.readiness.reason)
  return reason === null ? status : `${status} · ${reason}`
})
const readinessTone = computed(() =>
  overview.value === null ? 'neutral' : READINESS_STATUS_TONES[overview.value.readiness.status],
)
const pressureLabel = computed(() =>
  overview.value === null ? '' : PRESSURE_LABELS[overview.value.pressure],
)
const pressureTone = computed(() =>
  overview.value === null ? 'neutral' : PRESSURE_TONES[overview.value.pressure],
)

// Polling halts (and any in-flight request aborts) when the tab hides and
// resumes when it returns. The same teardown runs on unmount.
function handleVisibilityChange(): void {
  if (document.hidden) {
    stopClock()
    store.stop()
  } else {
    nowMs.value = Date.now()
    startClock()
    store.refresh()
  }
}

onMounted(() => {
  startClock()
  document.addEventListener('visibilitychange', handleVisibilityChange)
  store.start()
})

onUnmounted(() => {
  document.removeEventListener('visibilitychange', handleVisibilityChange)
  stopClock()
  unsubscribe()
  store.stop()
})
</script>

<template>
  <div class="app-shell">
    <header class="app-shell__header">
      <div class="app-shell__brand">
        <span class="app-shell__mark" aria-hidden="true"></span>
        <div class="app-shell__heading">
          <h1 class="app-shell__title">Station Overview</h1>
          <p class="app-shell__subtitle">BadTower operator console</p>
        </div>
      </div>
      <div v-if="phase !== 'credential'" class="app-shell__actions">
        <p
          v-if="freshnessText !== null"
          class="app-shell__freshness"
          :class="{ 'app-shell__freshness--stale': stale }"
        >
          Updated {{ freshnessText }} · every {{ pollSeconds }}&nbsp;s<template v-if="stale">
            · stale</template
          >
        </p>
        <button v-if="hasSnapshot" type="button" class="btn btn--ghost" @click="store.refresh()">
          Refresh
        </button>
        <button type="button" class="btn btn--ghost" @click="handleSignOut">Sign out</button>
      </div>
    </header>

    <main class="app-shell__main">
      <section
        v-if="phase === 'credential'"
        class="panel panel--gate"
        aria-labelledby="credential-gate-heading"
      >
        <h2 id="credential-gate-heading" class="panel__title">
          {{ PHASE_LABELS.credentialRequired }}
        </h2>
        <p class="panel__text">
          Enter this station's operator credential. It is held in memory only — never stored,
          logged, or shown again.
        </p>
        <CredentialGate :rejected="credentialRejected" @submit="handleCredentialSubmit" />
      </section>

      <section v-else-if="phase === 'loading'" class="panel panel--center" role="status">
        <span class="spinner" aria-hidden="true"></span>
        <p class="panel__text">{{ PHASE_LABELS.loading }}</p>
      </section>

      <section v-else-if="phase === 'unavailable'" class="panel panel--center" role="alert">
        <h2 class="panel__title">{{ PHASE_LABELS.unavailable }}</h2>
        <p class="panel__text">{{ PHASE_LABELS.unavailableDetail }}</p>
        <button type="button" class="btn btn--primary" @click="store.refresh()">Retry now</button>
      </section>

      <template v-else-if="overview !== null">
        <div v-if="storeState.halted" class="banner banner--halted" role="alert">
          <span>{{ PHASE_LABELS.unavailable }} — automatic polling paused.</span>
          <button type="button" class="btn btn--inline" @click="store.refresh()">Retry</button>
        </div>
        <div v-else-if="stale" class="banner banner--stale" role="status">
          {{ PHASE_LABELS.stale }}
        </div>

        <section class="status-row" aria-label="Station status">
          <StatusBadge :label="lifecycleLabel" :tone="lifecycleTone" />
          <StatusBadge :label="readinessLabel" :tone="readinessTone" />
          <StatusBadge :label="pressureLabel" :tone="pressureTone" />
        </section>

        <CapacityGrid :capacity="overview.capacity" />
      </template>

      <section v-else class="panel panel--center" role="status">
        <span class="spinner" aria-hidden="true"></span>
        <p class="panel__text">{{ PHASE_LABELS.loading }}</p>
      </section>
    </main>
  </div>
</template>
