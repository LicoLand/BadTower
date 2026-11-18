<script setup lang="ts">
import { ref } from 'vue'

const props = withDefaults(
  defineProps<{
    disabled?: boolean
    rejected?: boolean
  }>(),
  {
    disabled: false,
    rejected: false,
  },
)

const emit = defineEmits<{
  (event: 'submit', value: string): void
}>()

// The credential lives only in this ref for the lifetime of the keystroke.
// It is emitted once and immediately discarded: never persisted, logged, or rendered.
const inputValue = ref('')

function handleSubmit(): void {
  if (props.disabled) {
    return
  }
  const value = inputValue.value
  inputValue.value = ''
  emit('submit', value)
}
</script>

<template>
  <form class="credential-gate" @submit.prevent="handleSubmit">
    <label class="credential-gate__label" for="credential-gate-input">Credential</label>
    <input
      id="credential-gate-input"
      v-model="inputValue"
      class="credential-gate__input"
      type="password"
      autocomplete="off"
      :disabled="props.disabled"
      :aria-invalid="props.rejected || undefined"
      aria-describedby="credential-gate-feedback"
    />
    <p
      id="credential-gate-feedback"
      class="credential-gate__feedback"
      role="alert"
      :hidden="!props.rejected"
    >
      Credential rejected. Check the credential and try again.
    </p>
    <button class="credential-gate__submit" type="submit" :disabled="props.disabled">Unlock</button>
  </form>
</template>
