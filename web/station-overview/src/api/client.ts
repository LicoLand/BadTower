/**
 * Bounded client for `GET /operator/v1/overview`.
 *
 * Fail-closed guarantees: JSON content-type required, response size bounded,
 * strict contract parsing for both success and error payloads, one timeout
 * per request, external abort support, and typed outcomes — this function is
 * total and never throws.
 */

import { parseOverview, parseOverviewError, type Overview } from './overview'

export const OVERVIEW_ENDPOINT = '/operator/v1/overview'
export const REQUEST_TIMEOUT_MS = 4000
export const MAX_RESPONSE_BYTES = 16 * 1024

export type FetchOutcome =
  | { kind: 'ok'; overview: Overview }
  /** 401/403 — the caller wipes the credential. */
  | { kind: 'auth' }
  /** Valid retryable error payload, or 429/503; retryAfterMs may be absent. */
  | { kind: 'retryable'; retryAfterMs: number | null }
  /** Valid non-retryable error payload; polling halts until manual retry. */
  | { kind: 'fatal' }
  /** Network failure, timeout, abort, oversize, wrong content-type, bad JSON, contract violation. */
  | { kind: 'failed' }

export interface CredentialReader {
  getCredential(): string | null
}

export interface FetchOverviewOptions {
  session: CredentialReader
  signal?: AbortSignal
  fetchFn?: typeof fetch
  endpoint?: string
  timeoutMs?: number
  maxBytes?: number
}

export async function fetchOverview(options: FetchOverviewOptions): Promise<FetchOutcome> {
  const fetchFn = options.fetchFn ?? fetch
  const endpoint = options.endpoint ?? OVERVIEW_ENDPOINT
  const timeoutMs = options.timeoutMs ?? REQUEST_TIMEOUT_MS
  const maxBytes = options.maxBytes ?? MAX_RESPONSE_BYTES

  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)
  const external = options.signal ?? null
  const onExternalAbort = () => controller.abort()
  if (external !== null) {
    if (external.aborted) controller.abort()
    else external.addEventListener('abort', onExternalAbort, { once: true })
  }

  try {
    const headers: Record<string, string> = { Accept: 'application/json' }
    const credential = options.session.getCredential()
    if (credential !== null && credential !== '') {
      headers.Authorization = `Bearer ${credential}`
    }

    const response = await fetchFn(endpoint, {
      method: 'GET',
      headers,
      cache: 'no-store',
      credentials: 'same-origin',
      redirect: 'error',
      signal: controller.signal,
    })

    if (response.status === 401 || response.status === 403) {
      return { kind: 'auth' }
    }

    let payload: unknown
    const contentType = (response.headers.get('Content-Type') ?? '').toLowerCase()
    if (contentType.includes('application/json')) {
      const lengthHeader = response.headers.get('Content-Length')
      if (lengthHeader !== null) {
        const declared = Number(lengthHeader)
        if (Number.isFinite(declared) && declared > maxBytes) return { kind: 'failed' }
      }
      const text = await readBoundedText(response, maxBytes)
      if (text === null) return { kind: 'failed' }
      try {
        payload = JSON.parse(text)
      } catch {
        return { kind: 'failed' }
      }
    }

    if (response.status === 200) {
      if (payload === undefined) return { kind: 'failed' }
      const overview = parseOverview(payload)
      return overview === null ? { kind: 'failed' } : { kind: 'ok', overview }
    }

    if (payload !== undefined) {
      const failure = parseOverviewError(payload)
      if (failure !== null) {
        return failure.retryable
          ? { kind: 'retryable', retryAfterMs: failure.retryAfterMs }
          : { kind: 'fatal' }
      }
    }

    if (response.status === 429 || response.status === 503) {
      return { kind: 'retryable', retryAfterMs: parseRetryAfterMs(response.headers.get('Retry-After')) }
    }

    return { kind: 'failed' }
  } catch {
    return { kind: 'failed' }
  } finally {
    clearTimeout(timeout)
    external?.removeEventListener('abort', onExternalAbort)
  }
}

async function readBoundedText(response: Response, maxBytes: number): Promise<string | null> {
  const body = response.body
  if (body === null) {
    try {
      const text = await response.text()
      return new TextEncoder().encode(text).byteLength > maxBytes ? null : text
    } catch {
      return null
    }
  }
  const reader = body.getReader()
  const chunks: Uint8Array[] = []
  let received = 0
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      if (value !== undefined) {
        received += value.byteLength
        if (received > maxBytes) {
          await reader.cancel().catch(() => undefined)
          return null
        }
        chunks.push(value)
      }
    }
  } catch {
    return null
  }
  const merged = new Uint8Array(received)
  let offset = 0
  for (const chunk of chunks) {
    merged.set(chunk, offset)
    offset += chunk.byteLength
  }
  return new TextDecoder().decode(merged)
}

function parseRetryAfterMs(header: string | null): number | null {
  if (header === null) return null
  const seconds = Number(header)
  if (!Number.isFinite(seconds) || seconds < 0) return null
  return Math.round(seconds * 1000)
}
