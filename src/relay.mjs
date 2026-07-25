import { loadPinnedFabrigent } from "./fabrigent.mjs";

const constructionToken = Symbol("BadTowerRelay construction");

export class BadTowerRelay {
  static async create({ now = () => Date.now() } = {}) {
    const { policy } = await loadPinnedFabrigent();
    return new BadTowerRelay(constructionToken, policy.limits, now);
  }

  #mailboxes = new Map();
  #envelopeCount = 0;
  #limits;
  #now;

  constructor(token, limits, now) {
    if (token !== constructionToken) {
      throw new TypeError("BadTowerRelay must be created with BadTowerRelay.create()");
    }
    if (typeof now !== "function") {
      throw new TypeError("now must be a function");
    }
    this.#limits = limits;
    this.#now = now;
  }

  lease(mailboxId, requestedSeconds) {
    requireOpaqueId(mailboxId, "mailboxId");
    if (!Number.isInteger(requestedSeconds) || requestedSeconds <= 0) {
      throw new TypeError("lease duration must be a positive integer");
    }
    const leaseSeconds = Math.min(requestedSeconds, this.#limits.maxLeaseSeconds);
    const now = currentTime(this.#now);
    let mailbox = this.#mailboxes.get(mailboxId);
    if (mailbox?.leaseExpiresAt <= now) {
      this.#envelopeCount -= mailbox.envelopes.size;
      this.#mailboxes.delete(mailboxId);
      mailbox = undefined;
    }
    if (!mailbox) {
      if (this.#mailboxes.size >= this.#limits.maxMailboxes) {
        this.#pruneExpiredState(now);
      }
      if (this.#mailboxes.size >= this.#limits.maxMailboxes) {
        throw new Error("relay mailbox quota exceeded");
      }
      mailbox = { envelopes: new Map() };
    }
    mailbox.leaseExpiresAt = now + leaseSeconds * 1000;
    this.#mailboxes.set(mailboxId, mailbox);
    return { mailboxId, leaseExpiresAt: mailbox.leaseExpiresAt };
  }

  deliver(envelope) {
    const now = currentTime(this.#now);
    const validated = validateEnvelope(envelope, this.#limits, now);
    const mailbox = this.#activeMailbox(envelope.mailboxId, now);
    this.#removeExpiredEnvelopes(mailbox, now);
    const existing = mailbox.envelopes.get(envelope.envelopeId);
    if (existing) {
      if (!sameEnvelope(existing, validated)) {
        throw new Error("envelope identifier conflicts with stored content");
      }
      return { accepted: true, duplicate: true };
    }
    if (mailbox.envelopes.size >= this.#limits.maxMailboxEnvelopes) {
      throw new Error("mailbox quota exceeded");
    }
    if (this.#envelopeCount >= this.#limits.maxRelayEnvelopes) {
      this.#pruneExpiredState(now);
    }
    if (this.#envelopeCount >= this.#limits.maxRelayEnvelopes) {
      throw new Error("relay envelope quota exceeded");
    }
    mailbox.envelopes.set(envelope.envelopeId, validated);
    this.#envelopeCount += 1;
    return { accepted: true, duplicate: false };
  }

  receive(mailboxId, limit = 100) {
    const now = currentTime(this.#now);
    const mailbox = this.#activeMailbox(mailboxId, now);
    if (!Number.isInteger(limit) || limit <= 0 || limit > 100) {
      throw new RangeError("receive limit must be an integer from 1 to 100");
    }
    this.#removeExpiredEnvelopes(mailbox, now);
    return [...mailbox.envelopes.values()]
      .slice(0, limit)
      .map((envelope) => ({ ...envelope }));
  }

  acknowledge(mailboxId, envelopeId) {
    const now = currentTime(this.#now);
    const mailbox = this.#activeMailbox(mailboxId, now);
    requireOpaqueId(envelopeId, "envelopeId");
    this.#removeExpiredEnvelopes(mailbox, now);
    const acknowledged = mailbox.envelopes.delete(envelopeId);
    if (acknowledged) this.#envelopeCount -= 1;
    return { acknowledged };
  }

  cleanup() {
    return this.#pruneExpiredState(currentTime(this.#now));
  }

  #activeMailbox(mailboxId, now) {
    requireOpaqueId(mailboxId, "mailboxId");
    const mailbox = this.#mailboxes.get(mailboxId);
    if (!mailbox || mailbox.leaseExpiresAt <= now) {
      throw new Error("mailbox lease is missing or expired");
    }
    return mailbox;
  }

  #removeExpiredEnvelopes(mailbox, now) {
    let removed = 0;
    for (const [envelopeId, envelope] of mailbox.envelopes) {
      if (Date.parse(envelope.expiresAt) <= now) {
        mailbox.envelopes.delete(envelopeId);
        this.#envelopeCount -= 1;
        removed += 1;
      }
    }
    return removed;
  }

  #pruneExpiredState(now) {
    let removedEnvelopes = 0;
    let removedMailboxes = 0;
    for (const [mailboxId, mailbox] of this.#mailboxes) {
      removedEnvelopes += this.#removeExpiredEnvelopes(mailbox, now);
      if (mailbox.leaseExpiresAt <= now) {
        this.#envelopeCount -= mailbox.envelopes.size;
        removedEnvelopes += mailbox.envelopes.size;
        this.#mailboxes.delete(mailboxId);
        removedMailboxes += 1;
      }
    }
    return { removedEnvelopes, removedMailboxes };
  }
}

function validateEnvelope(value, limits, now) {
  if (!value || typeof value !== "object" || Array.isArray(value) ||
      ![Object.prototype, null].includes(Object.getPrototypeOf(value))) {
    throw new TypeError("envelope must be an object");
  }
  const allowed = new Set([
    "contractVersion", "envelopeId", "mailboxId", "ciphertext", "expiresAt"
  ]);
  const keys = Reflect.ownKeys(value);
  if (keys.some((key) => typeof key !== "string" || !allowed.has(key))) {
    throw new TypeError("envelope contains a forbidden field");
  }
  if (keys.length !== allowed.size ||
      keys.some((key) => {
        const descriptor = Object.getOwnPropertyDescriptor(value, key);
        return !descriptor.enumerable || !Object.hasOwn(descriptor, "value");
      })) {
    throw new TypeError("envelope fields must be enumerable data properties");
  }
  if (value.contractVersion !== "fabrigent.relay.v2") {
    throw new TypeError("unsupported Fabrigent relay contract");
  }
  requireOpaqueId(value.envelopeId, "envelopeId");
  requireOpaqueId(value.mailboxId, "mailboxId");
  if (typeof value.ciphertext !== "string" || value.ciphertext.length === 0) {
    throw new TypeError("ciphertext is required");
  }
  if (Buffer.byteLength(value.ciphertext, "utf8") > limits.maxCiphertextBytes) {
    throw new RangeError("ciphertext quota exceeded");
  }
  if (!isRfc3339DateTime(value.expiresAt)) {
    throw new TypeError("expiresAt must be an RFC 3339 date-time");
  }
  const expiresAt = Date.parse(value.expiresAt);
  if (expiresAt <= now) {
    throw new TypeError("expiresAt must be a future date-time");
  }
  if (expiresAt - now > limits.maxEnvelopeRetentionSeconds * 1000) {
    throw new RangeError("envelope retention quota exceeded");
  }
  return Object.freeze({
    contractVersion: value.contractVersion,
    envelopeId: value.envelopeId,
    mailboxId: value.mailboxId,
    ciphertext: value.ciphertext,
    expiresAt: value.expiresAt
  });
}

function requireOpaqueId(value, field) {
  if (typeof value !== "string" || !/^[A-Za-z0-9_-]{16,128}$/.test(value)) {
    throw new TypeError(`${field} is not a valid opaque identifier`);
  }
}

function currentTime(now) {
  const value = now();
  if (!Number.isSafeInteger(value)) {
    throw new TypeError("now must return a safe integer timestamp");
  }
  return value;
}

function isRfc3339DateTime(value) {
  if (typeof value !== "string") return false;
  const match = /^(?<year>\d{4})-(?<month>\d{2})-(?<day>\d{2})[Tt](?<hour>\d{2}):(?<minute>\d{2}):(?<second>\d{2})(?:\.\d+)?(?<zone>[Zz]|[+-]\d{2}:\d{2})$/.exec(value);
  if (!match) return false;
  const fields = Object.fromEntries(
    ["year", "month", "day", "hour", "minute", "second"]
      .map((name) => [name, Number(match.groups[name])])
  );
  const leapYear = fields.year % 4 === 0 &&
    (fields.year % 100 !== 0 || fields.year % 400 === 0);
  const daysInMonth = [
    31, leapYear ? 29 : 28, 31, 30, 31, 30,
    31, 31, 30, 31, 30, 31
  ][fields.month - 1];
  if (fields.month < 1 || fields.month > 12 ||
      fields.day < 1 || fields.day > daysInMonth ||
      fields.hour > 23 || fields.minute > 59 || fields.second > 59) {
    return false;
  }
  if (match.groups.zone !== "Z" && match.groups.zone !== "z") {
    const [offsetHour, offsetMinute] = match.groups.zone.slice(1).split(":").map(Number);
    if (offsetHour > 23 || offsetMinute > 59) return false;
  }
  return Number.isFinite(Date.parse(value));
}

function sameEnvelope(left, right) {
  return left.contractVersion === right.contractVersion &&
    left.envelopeId === right.envelopeId &&
    left.mailboxId === right.mailboxId &&
    left.ciphertext === right.ciphertext &&
    left.expiresAt === right.expiresAt;
}
