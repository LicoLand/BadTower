import { loadPinnedFabrigent } from "./fabrigent.mjs";

export class BadTowerRelay {
  static async create({ now = () => Date.now() } = {}) {
    const { policy } = await loadPinnedFabrigent();
    return new BadTowerRelay(policy.limits, now);
  }

  #mailboxes = new Map();
  #limits;
  #now;

  constructor(limits, now) {
    this.#limits = limits;
    this.#now = now;
  }

  lease(mailboxId, requestedSeconds) {
    requireOpaqueId(mailboxId, "mailboxId");
    const leaseSeconds = Math.min(requestedSeconds, this.#limits.maxLeaseSeconds);
    if (!Number.isInteger(leaseSeconds) || leaseSeconds <= 0) {
      throw new TypeError("lease duration must be a positive integer");
    }
    const mailbox = this.#mailboxes.get(mailboxId) ?? { envelopes: new Map() };
    mailbox.leaseExpiresAt = this.#now() + leaseSeconds * 1000;
    this.#mailboxes.set(mailboxId, mailbox);
    return { mailboxId, leaseExpiresAt: mailbox.leaseExpiresAt };
  }

  deliver(envelope) {
    validateEnvelope(envelope, this.#limits, this.#now());
    const mailbox = this.#activeMailbox(envelope.mailboxId);
    if (mailbox.envelopes.size >= this.#limits.maxMailboxEnvelopes) {
      throw new Error("mailbox quota exceeded");
    }
    if (mailbox.envelopes.has(envelope.envelopeId)) {
      return { accepted: true, duplicate: true };
    }
    mailbox.envelopes.set(envelope.envelopeId, structuredClone(envelope));
    return { accepted: true, duplicate: false };
  }

  receive(mailboxId, limit = 100) {
    const mailbox = this.#activeMailbox(mailboxId);
    if (!Number.isInteger(limit) || limit <= 0 || limit > 100) {
      throw new RangeError("receive limit must be an integer from 1 to 100");
    }
    return [...mailbox.envelopes.values()]
      .filter(({ expiresAt }) => Date.parse(expiresAt) > this.#now())
      .slice(0, limit)
      .map((envelope) => structuredClone(envelope));
  }

  acknowledge(mailboxId, envelopeId) {
    const mailbox = this.#activeMailbox(mailboxId);
    requireOpaqueId(envelopeId, "envelopeId");
    return { acknowledged: mailbox.envelopes.delete(envelopeId) };
  }

  cleanup() {
    const now = this.#now();
    let removedEnvelopes = 0;
    let removedMailboxes = 0;
    for (const [mailboxId, mailbox] of this.#mailboxes) {
      for (const [envelopeId, envelope] of mailbox.envelopes) {
        if (Date.parse(envelope.expiresAt) <= now) {
          mailbox.envelopes.delete(envelopeId);
          removedEnvelopes += 1;
        }
      }
      if (mailbox.leaseExpiresAt <= now) {
        this.#mailboxes.delete(mailboxId);
        removedMailboxes += 1;
      }
    }
    return { removedEnvelopes, removedMailboxes };
  }

  #activeMailbox(mailboxId) {
    requireOpaqueId(mailboxId, "mailboxId");
    const mailbox = this.#mailboxes.get(mailboxId);
    if (!mailbox || mailbox.leaseExpiresAt <= this.#now()) {
      throw new Error("mailbox lease is missing or expired");
    }
    return mailbox;
  }
}

function validateEnvelope(value, limits, now) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("envelope must be an object");
  }
  const allowed = new Set([
    "contractVersion", "envelopeId", "mailboxId", "ciphertext", "expiresAt"
  ]);
  if (Object.keys(value).some((key) => !allowed.has(key))) {
    throw new TypeError("envelope contains a forbidden field");
  }
  if (value.contractVersion !== "fabrigent.relay.v1") {
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
  const expiresAt = Date.parse(value.expiresAt);
  if (!Number.isFinite(expiresAt) || expiresAt <= now) {
    throw new TypeError("expiresAt must be a future date-time");
  }
}

function requireOpaqueId(value, field) {
  if (typeof value !== "string" || !/^[A-Za-z0-9_-]{16,128}$/.test(value)) {
    throw new TypeError(`${field} is not a valid opaque identifier`);
  }
}
