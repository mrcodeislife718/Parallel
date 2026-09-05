export class PermissionAuditLog {
  constructor({ maxEntries = 2048, onViolation = null } = {}) {
    if (!Number.isInteger(maxEntries) || maxEntries < 1) throw new TypeError('maxEntries must be a positive integer');
    if (onViolation != null && typeof onViolation !== 'function') throw new TypeError('onViolation must be a function');
    this.maxEntries = maxEntries;
    this.onViolation = onViolation;
    this.entries = [];
    this.sequence = 0;
  }

  record(entry) {
    const event = Object.freeze({ sequence: ++this.sequence, timestamp: new Date().toISOString(), ...entry });
    this.entries.push(event);
    if (this.entries.length > this.maxEntries) this.entries.splice(0, this.entries.length - this.maxEntries);
    this.onViolation?.(event);
    return event;
  }

  snapshot() { return this.entries.map((entry) => ({ ...entry })); }
  clear() { this.entries.length = 0; }
}

export class PermissionPolicy {
  constructor({ mode = 'enforce', deny = {}, audit = {} } = {}) {
    if (!['enforce', 'audit'].includes(mode)) throw new TypeError("permission mode must be 'enforce' or 'audit'");
    this.mode = mode;
    this.denies = new Map();
    for (const [capability, values] of Object.entries(deny ?? {})) this.denies.set(capability, normalizeRule(values));
    this.audit = new PermissionAuditLog(audit);
  }

  isExplicitlyDenied(capability, resource = null) {
    const exact = this.denies.get(capability);
    const family = this.denies.get(capability.split('.')[0]);
    return matchesRule(exact, resource) || matchesRule(family, resource);
  }

  decide({ capability, resource = null, allowed }) {
    const explicitDeny = this.isExplicitlyDenied(capability, resource);
    const granted = Boolean(allowed) && !explicitDeny;
    if (!granted) {
      this.audit.record({ capability, resource, reason: explicitDeny ? 'explicit-deny' : 'not-allowed', mode: this.mode });
      if (this.mode === 'audit') return Object.freeze({ state: 'granted', audited: true, wouldDeny: true, reason: explicitDeny ? 'explicit-deny' : 'not-allowed' });
      return Object.freeze({ state: 'denied', audited: false, wouldDeny: true, reason: explicitDeny ? 'explicit-deny' : 'not-allowed' });
    }
    return Object.freeze({ state: 'granted', audited: false, wouldDeny: false, reason: null });
  }

  snapshot() {
    return {
      mode: this.mode,
      deny: Object.fromEntries([...this.denies.entries()].map(([key, value]) => [key, value === true ? true : [...value].sort()])),
      audit: this.audit.snapshot(),
    };
  }
}

function normalizeRule(value) {
  if (value === true) return true;
  if (!value) return new Set();
  if (!Array.isArray(value)) throw new TypeError('deny permission rules must be boolean or arrays');
  return new Set(value.map(String));
}

function matchesRule(rule, resource) {
  if (rule === true) return true;
  if (!(rule instanceof Set) || rule.size === 0) return false;
  if (rule.has('*')) return true;
  if (resource == null) return false;
  const value = String(resource);
  if (rule.has(value)) return true;
  for (const candidate of rule) {
    if (candidate.endsWith('/') && value.startsWith(candidate)) return true;
  }
  return false;
}
