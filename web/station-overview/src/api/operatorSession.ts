/**
 * Memory-only operator credential session.
 *
 * The bearer credential lives exclusively inside this closure for the current
 * page lifetime. It is never written to localStorage, sessionStorage, cookies,
 * IndexedDB, the URL, logs, or the DOM. There is no "remember me". A reload
 * means re-entry. The variable itself is never exported.
 */

export interface OperatorSession {
  /** Store one non-empty credential, replacing any previous one. */
  setCredential(credential: string): void
  /** Read the credential at request time only. */
  getCredential(): string | null
  /** Wipe the credential (sign-out, 401/403, teardown). */
  clear(): void
  hasCredential(): boolean
}

export function createOperatorSession(): OperatorSession {
  let credential: string | null = null
  return {
    setCredential(value: string) {
      credential = typeof value === 'string' && value.length > 0 ? value : null
    },
    getCredential() {
      return credential
    },
    clear() {
      credential = null
    },
    hasCredential() {
      return credential !== null
    },
  }
}
