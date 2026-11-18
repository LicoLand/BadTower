import { afterEach, describe, expect, it, vi } from 'vitest'

import canonicalFixture from '../../contracts/overview.fixture.json'
import errorFixture from '../../contracts/overview-error.fixture.json'
import {
  fetchOverview,
  MAX_RESPONSE_BYTES,
  OVERVIEW_ENDPOINT,
  REQUEST_TIMEOUT_MS,
} from './client'
import { createOperatorSession } from './operatorSession'
import { OVERVIEW_ERROR_SCHEMA, parseOverview } from './overview'

const expectedOverview = parseOverview(structuredClone(canonicalFixture))
if (expectedOverview === null) throw new Error('canonical fixture must parse')

function jsonResponse(
  payload: unknown,
  init?: { status?: number; headers?: Record<string, string> },
): Response {
  return new Response(JSON.stringify(payload), {
    status: init?.status ?? 200,
    headers: { 'Content-Type': 'application/json', ...(init?.headers ?? {}) },
  })
}

afterEach(() => {
  vi.useRealTimers()
})

describe('fetchOverview request shape', () => {
  it('sends Accept, Bearer Authorization, and no-store on the same-origin GET', async () => {
    const session = createOperatorSession()
    session.setCredential('test-credential')
    let seenInput: unknown
    let seenInit: RequestInit | undefined
    const fetchFn: typeof fetch = async (input, init) => {
      seenInput = input
      seenInit = init
      return jsonResponse(canonicalFixture)
    }

    const outcome = await fetchOverview({ session, fetchFn })

    expect(outcome).toEqual({ kind: 'ok', overview: expectedOverview })
    expect(seenInput).toBe(OVERVIEW_ENDPOINT)
    const headers = seenInit?.headers as Record<string, string>
    expect(headers.Accept).toBe('application/json')
    expect(headers.Authorization).toBe('Bearer test-credential')
    expect(seenInit?.method).toBe('GET')
    expect(seenInit?.cache).toBe('no-store')
    expect(seenInit?.credentials).toBe('same-origin')
  })

  it('never exposes the credential in the outcome', async () => {
    const session = createOperatorSession()
    session.setCredential('test-credential')
    const fetchFn: typeof fetch = async () => jsonResponse(canonicalFixture)
    const outcome = await fetchOverview({ session, fetchFn })
    expect(JSON.stringify(outcome)).not.toContain('test-credential')
  })

  it('omits Authorization when no credential is held', async () => {
    const session = createOperatorSession()
    let seenInit: RequestInit | undefined
    const fetchFn: typeof fetch = async (_input, init) => {
      seenInit = init
      return new Response(null, { status: 401 })
    }
    const outcome = await fetchOverview({ session, fetchFn })
    expect(outcome).toEqual({ kind: 'auth' })
    const headers = seenInit?.headers as Record<string, string>
    expect('Authorization' in headers).toBe(false)
  })
})

describe('fetchOverview status and payload handling', () => {
  const session = () => {
    const s = createOperatorSession()
    s.setCredential('test-credential')
    return s
  }

  it('maps 401 and 403 to auth', async () => {
    for (const status of [401, 403]) {
      const fetchFn: typeof fetch = async () => new Response(null, { status })
      await expect(fetchOverview({ session: session(), fetchFn })).resolves.toEqual({ kind: 'auth' })
    }
  })

  it('rejects a contract-invalid 200 payload', async () => {
    const fetchFn: typeof fetch = async () =>
      jsonResponse({ ...structuredClone(canonicalFixture), stationName: 'tower-1' })
    await expect(fetchOverview({ session: session(), fetchFn })).resolves.toEqual({ kind: 'failed' })
  })

  it('maps a valid retryable error payload to retryable with retryAfterMs', async () => {
    const fetchFn: typeof fetch = async () => jsonResponse(errorFixture, { status: 503 })
    await expect(fetchOverview({ session: session(), fetchFn })).resolves.toEqual({
      kind: 'retryable',
      retryAfterMs: 15000,
    })
  })

  it('maps a valid non-retryable error payload to fatal', async () => {
    const payload = {
      schema: OVERVIEW_ERROR_SCHEMA,
      reason: 'INTERNAL',
      retryable: false,
      retryAfterMs: 0,
    }
    const fetchFn: typeof fetch = async () => jsonResponse(payload, { status: 500 })
    await expect(fetchOverview({ session: session(), fetchFn })).resolves.toEqual({ kind: 'fatal' })
  })

  it('honors Retry-After on 429/503 when the error body is not a valid error payload', async () => {
    const fetchFn: typeof fetch = async () =>
      new Response('temporarily busy', {
        status: 503,
        headers: { 'Content-Type': 'text/plain', 'Retry-After': '20' },
      })
    await expect(fetchOverview({ session: session(), fetchFn })).resolves.toEqual({
      kind: 'retryable',
      retryAfterMs: 20000,
    })

    const noHeader: typeof fetch = async () =>
      new Response('busy', { status: 429, headers: { 'Content-Type': 'text/plain' } })
    await expect(fetchOverview({ session: session(), fetchFn: noHeader })).resolves.toEqual({
      kind: 'retryable',
      retryAfterMs: null,
    })
  })

  it('rejects a 200 with a non-JSON content type', async () => {
    const fetchFn: typeof fetch = async () =>
      new Response(JSON.stringify(canonicalFixture), {
        status: 200,
        headers: { 'Content-Type': 'text/html' },
      })
    await expect(fetchOverview({ session: session(), fetchFn })).resolves.toEqual({ kind: 'failed' })
  })

  it('rejects an oversized declared Content-Length before reading', async () => {
    const fetchFn: typeof fetch = async () =>
      jsonResponse(canonicalFixture, {
        headers: { 'Content-Length': String(MAX_RESPONSE_BYTES + 1) },
      })
    await expect(fetchOverview({ session: session(), fetchFn })).resolves.toEqual({ kind: 'failed' })
  })

  it('rejects an oversized streamed body while reading', async () => {
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(9000))
        controller.enqueue(new Uint8Array(9000))
        controller.close()
      },
    })
    const fetchFn: typeof fetch = async () =>
      new Response(stream, { status: 200, headers: { 'Content-Type': 'application/json' } })
    await expect(fetchOverview({ session: session(), fetchFn })).resolves.toEqual({ kind: 'failed' })
  })

  it('rejects invalid JSON', async () => {
    const fetchFn: typeof fetch = async () =>
      new Response('{not json', { status: 200, headers: { 'Content-Type': 'application/json' } })
    await expect(fetchOverview({ session: session(), fetchFn })).resolves.toEqual({ kind: 'failed' })
  })

  it('maps network failure to failed', async () => {
    const fetchFn: typeof fetch = async () => {
      throw new TypeError('network down')
    }
    await expect(fetchOverview({ session: session(), fetchFn })).resolves.toEqual({ kind: 'failed' })
  })

  it('maps request timeout to failed', async () => {
    vi.useFakeTimers()
    const fetchFn: typeof fetch = (_input, init) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () =>
          reject(new DOMException('Aborted', 'AbortError')),
        )
      })
    const promise = fetchOverview({ session: session(), fetchFn })
    await vi.advanceTimersByTimeAsync(REQUEST_TIMEOUT_MS)
    await expect(promise).resolves.toEqual({ kind: 'failed' })
  })
})
