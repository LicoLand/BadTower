/**
 * Deterministic unit tests for the station-overview polling store.
 *
 * Written against the store's documented contract only (the implementation
 * was not consulted). Assumed shapes are kept in exactly one place — the
 * fixtures below — so any contract drift is a one-spot fix:
 *   - deps: { session, fetchOnce?, now? }
 *   - fetchOnce(signal) resolves to a FetchOutcome:
 *       { kind: 'ok', overview } | { kind: 'auth' }
 *       | { kind: 'retryable', retryAfterMs } | { kind: 'fatal' | 'failed' }
 *   - getState() exposes at least `phase`; the no-credential phase is
 *     'credential'
 *   - getOverview() returns the last successful overview, null-ish before
 *     any success and after reset
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { FetchOutcome } from '../api/client';
import type { Overview } from '../api/overview';

import {
  createOverviewStore,
  DEFAULT_POLL_MS,
  MAX_POLL_MS,
  MIN_POLL_MS,
  STALE_MIN_MS,
} from './overview';

// ---------------------------------------------------------------------------
// Local fixtures
// ---------------------------------------------------------------------------

const CREDENTIAL = crypto.randomUUID();
const OVERVIEW: Overview = {
  schema: 'badtower.station-overview.v1',
  lifecycle: 'RUNNING',
  readiness: { status: 'SERVING', reason: 'NONE' },
  pressure: 'NORMAL',
  capacity: {
    configuredNamespaceCount: 1,
    implementationCeiling: {
      namespaceCount: 4,
      perNamespace: {
        addresses: 1024,
        transportUnits: 256,
        logicalRetainedBytes: 1048576,
        activeClaims: 128,
      },
    },
    aggregateUsage: {
      addresses: 8,
      transportUnits: 2,
      logicalRetainedBytes: 4096,
      activeClaims: 1,
    },
  },
  pollAfterMs: DEFAULT_POLL_MS,
};

function okOutcome(overview: Overview = OVERVIEW): FetchOutcome {
  return { kind: 'ok', overview };
}

function authOutcome(): FetchOutcome {
  return { kind: 'auth' };
}

function retryableOutcome(retryAfterMs: number | null = null): FetchOutcome {
  return { kind: 'retryable', retryAfterMs };
}

function fatalOutcome(): FetchOutcome {
  return { kind: 'fatal' };
}

type TestOutcome = FetchOutcome;

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
}

function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function createSession() {
  let credential: string | null = null;
  return {
    setCredential: vi.fn((next: string) => {
      credential = next;
    }),
    getCredential: vi.fn((): string | null => credential),
    clear: vi.fn(() => {
      credential = null;
    }),
    hasCredential: vi.fn(() => credential !== null),
  };
}

interface FetchCall {
  signal: AbortSignal;
  deferred: Deferred<TestOutcome>;
}

function setup() {
  const session = createSession();
  const calls: FetchCall[] = [];
  const fetchOnce = vi.fn((signal: AbortSignal): Promise<TestOutcome> => {
    const deferred = createDeferred<TestOutcome>();
    calls.push({ signal, deferred });
    return deferred.promise;
  });
  const now = () => Date.now();
  const store = createOverviewStore({ session, fetchOnce, now });
  return { session, fetchOnce, calls, store };
}

/** Flush pending promise continuations without advancing the clock. */
async function flush(): Promise<void> {
  for (let i = 0; i < 3; i += 1) {
    await Promise.resolve();
  }
  await vi.advanceTimersByTimeAsync(0);
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe('createOverviewStore', () => {
  describe('poll interval constants', () => {
    it('orders the clamp bounds around the default and below the stale threshold', () => {
      expect(MIN_POLL_MS).toBeGreaterThan(0);
      expect(MIN_POLL_MS).toBeLessThanOrEqual(DEFAULT_POLL_MS);
      expect(DEFAULT_POLL_MS).toBeLessThanOrEqual(MAX_POLL_MS);
      // the freshness tests assume the default poll fires before staleness
      expect(DEFAULT_POLL_MS).toBeLessThan(STALE_MIN_MS);
    });
  });

  describe('credential gating', () => {
    it('stays in the credential state and never fetches without a credential', async () => {
      const { store, session, fetchOnce } = setup();
      expect(session.hasCredential()).toBe(false);

      store.start();

      expect(store.getState().phase).toBe('credential');
      expect(store.isPolling()).toBe(false);
      expect(store.isInFlight()).toBe(false);
      expect(fetchOnce).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(MAX_POLL_MS * 2);
      expect(fetchOnce).not.toHaveBeenCalled();
      expect(store.getState().phase).toBe('credential');
    });
  });

  describe('start', () => {
    it('fetches immediately and stays single-flight while a fetch is in flight', async () => {
      const { store, session, fetchOnce, calls } = setup();
      session.setCredential(CREDENTIAL);

      store.start();

      // immediate: the first fetch needs no timer advance
      expect(fetchOnce).toHaveBeenCalledTimes(1);
      expect(store.isInFlight()).toBe(true);
      expect(calls[0].signal.aborted).toBe(false);

      // repeated start/refresh must not spawn a concurrent fetch
      store.start();
      store.refresh();
      await vi.advanceTimersByTimeAsync(MIN_POLL_MS);
      expect(fetchOnce).toHaveBeenCalledTimes(1);
    });
  });

  describe('successful fetch', () => {
    it('applies the overview, reschedules at the clamped default interval, and tracks staleness', async () => {
      const { store, session, fetchOnce, calls } = setup();
      session.setCredential(CREDENTIAL);
      store.start();

      calls[0].deferred.resolve(okOutcome());
      await flush();

      expect(store.isInFlight()).toBe(false);
      expect(store.isPolling()).toBe(true);
      expect(store.getOverview()).toEqual(OVERVIEW);
      expect(store.isStale()).toBe(false);

      // the follow-up poll is clamped to the default interval: never early ...
      await vi.advanceTimersByTimeAsync(DEFAULT_POLL_MS - 1);
      expect(fetchOnce).toHaveBeenCalledTimes(1);
      // ... and fires exactly on schedule
      await vi.advanceTimersByTimeAsync(1);
      expect(fetchOnce).toHaveBeenCalledTimes(2);

      // freshness: not stale inside the stale window measured from the success ...
      await vi.advanceTimersByTimeAsync(STALE_MIN_MS - 1 - DEFAULT_POLL_MS);
      expect(store.isStale()).toBe(false);
      // ... stale once the window has passed without a new success
      await vi.advanceTimersByTimeAsync(2);
      expect(store.isStale()).toBe(true);
    });
  });

  describe('retryable outcome', () => {
    it('keeps polling and retries within the clamped maximum interval', async () => {
      const { store, session, fetchOnce, calls } = setup();
      session.setCredential(CREDENTIAL);
      store.start();

      calls[0].deferred.resolve(retryableOutcome());
      await flush();

      expect(store.isInFlight()).toBe(false);
      expect(store.isPolling()).toBe(true);

      await vi.advanceTimersByTimeAsync(MAX_POLL_MS);
      expect(fetchOnce).toHaveBeenCalledTimes(2);
    });
  });

  describe('auth outcome', () => {
    it('clears the credential and halts polling', async () => {
      const { store, session, fetchOnce, calls } = setup();
      session.setCredential(CREDENTIAL);
      store.start();

      calls[0].deferred.resolve(authOutcome());
      await flush();

      expect(session.clear).toHaveBeenCalledTimes(1);
      expect(session.hasCredential()).toBe(false);
      expect(store.getState().phase).toBe('credential');
      expect(store.isPolling()).toBe(false);
      expect(store.isInFlight()).toBe(false);

      // halted: no further fetch is ever scheduled
      await vi.advanceTimersByTimeAsync(MAX_POLL_MS * 2);
      expect(fetchOnce).toHaveBeenCalledTimes(1);
    });
  });

  describe('fatal outcome', () => {
    it('halts without scheduling a poll, and a manual refresh starts exactly one new fetch', async () => {
      const { store, session, fetchOnce, calls } = setup();
      session.setCredential(CREDENTIAL);
      store.start();

      calls[0].deferred.resolve(fatalOutcome());
      await flush();

      expect(store.getState().halted).toBe(true);
      expect(store.isPolling()).toBe(false);
      expect(store.isInFlight()).toBe(false);

      // halted: no poll is ever scheduled on its own
      await vi.advanceTimersByTimeAsync(MAX_POLL_MS * 2);
      expect(fetchOnce).toHaveBeenCalledTimes(1);

      // manual refresh restarts the loop with a single new fetch
      store.refresh();
      expect(fetchOnce).toHaveBeenCalledTimes(2);
      expect(store.isInFlight()).toBe(true);
      expect(store.getState().halted).toBe(false);

      calls[1].deferred.resolve(okOutcome());
      await flush();
      expect(store.isInFlight()).toBe(false);
    });
  });

  describe('stop', () => {
    it('aborts the in-flight fetch and a late completion cannot schedule another poll', async () => {
      const { store, session, fetchOnce, calls } = setup();
      session.setCredential(CREDENTIAL);
      store.start();
      expect(fetchOnce).toHaveBeenCalledTimes(1);

      store.stop();

      expect(calls[0].signal.aborted).toBe(true);
      expect(store.isPolling()).toBe(false);
      // the abort does not settle the fetch: still in flight until it resolves
      expect(store.isInFlight()).toBe(true);

      // the in-flight fetch resolves after the abort: nothing may be scheduled
      calls[0].deferred.resolve(okOutcome());
      await flush();
      expect(store.isInFlight()).toBe(false);
      await vi.advanceTimersByTimeAsync(MAX_POLL_MS * 2);
      expect(fetchOnce).toHaveBeenCalledTimes(1);
    });
  });

  describe('reset', () => {
    it('clears credential and state, and a late completion cannot resurrect the store', async () => {
      const { store, session, fetchOnce, calls } = setup();
      session.setCredential(CREDENTIAL);
      store.start();
      calls[0].deferred.resolve(okOutcome());
      await flush();
      expect(store.getOverview()).toEqual(OVERVIEW);

      // leave a second fetch in flight so reset must deal with it
      await vi.advanceTimersByTimeAsync(DEFAULT_POLL_MS);
      expect(fetchOnce).toHaveBeenCalledTimes(2);

      store.reset();

      expect(session.clear).toHaveBeenCalledTimes(1);
      expect(session.hasCredential()).toBe(false);
      expect(store.getOverview() ?? null).toBeNull();
      expect(store.getState().phase).toBe('credential');
      expect(store.isPolling()).toBe(false);
      // like stop(): reset aborts the in-flight fetch but cannot settle it
      expect(calls[1].signal.aborted).toBe(true);
      expect(store.isInFlight()).toBe(true);

      // late completion of the pre-reset fetch must not resurrect anything
      calls[1].deferred.resolve(okOutcome({ ...OVERVIEW, pressure: 'HARD' }));
      await flush();
      expect(store.isInFlight()).toBe(false);
      await vi.advanceTimersByTimeAsync(MAX_POLL_MS * 2);
      expect(store.getOverview() ?? null).toBeNull();
      expect(store.getState().phase).toBe('credential');
      expect(fetchOnce).toHaveBeenCalledTimes(2);
    });
  });

  describe('refresh', () => {
    it('is a no-op while a fetch is already in flight', async () => {
      const { store, session, fetchOnce, calls } = setup();
      session.setCredential(CREDENTIAL);
      store.start();
      expect(fetchOnce).toHaveBeenCalledTimes(1);
      expect(store.isInFlight()).toBe(true);

      // refresh must not spawn a concurrent fetch or disturb the active one
      store.refresh();
      store.refresh();
      await vi.advanceTimersByTimeAsync(MIN_POLL_MS);
      expect(fetchOnce).toHaveBeenCalledTimes(1);
      expect(store.isInFlight()).toBe(true);
      expect(calls[0].signal.aborted).toBe(false);

      calls[0].deferred.resolve(okOutcome());
      await flush();
      expect(store.isInFlight()).toBe(false);
    });
  });

  describe('subscribe', () => {
    it('notifies listeners on state changes until unsubscribed', async () => {
      const { store, session, calls } = setup();
      const listener = vi.fn();
      const unsubscribe = store.subscribe(listener);

      session.setCredential(CREDENTIAL);
      store.start();
      expect(listener).toHaveBeenCalled();

      calls[0].deferred.resolve(okOutcome());
      await flush();
      const notified = listener.mock.calls.length;
      expect(notified).toBeGreaterThanOrEqual(2);

      unsubscribe();
      store.stop();
      await flush();
      expect(listener).toHaveBeenCalledTimes(notified);
    });
  });
});
