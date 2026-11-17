'use strict';

const STATE_CLOSED = 'closed';
const STATE_OPEN = 'open';
const STATE_HALF_OPEN = 'half-open';

const DEFAULTS = {
  failureThreshold: 3,        // consecutive errors that trip the breaker
  windowSize: 10,             // sliding window (ticks) for health stats
  errorRateThreshold: 0.5,    // error ratio inside the window that trips the breaker
  windowMinRequests: 5,       // minimum window volume before the error rate is considered
  openDuration: 5,            // ticks spent open before a probe is allowed
  halfOpenMaxProbes: 2,       // concurrent probes permitted while half-open
  halfOpenSuccessThreshold: 2,// consecutive probe successes required to close again
  rampUpTicks: 3,             // ticks to linearly restore traffic after recovery
  maxConcurrency: 10,         // in-flight cap per endpoint
};

/**
 * Per-endpoint circuit breaker. Time never advances on its own: the owner
 * must call tick() exactly once per logical time step.
 */
class CircuitBreaker {
  constructor(id, options = {}) {
    this.id = id;
    this.config = { ...DEFAULTS, ...options };
    this.state = STATE_CLOSED;
    this.now = 0;

    this.inFlight = 0;
    this.consecutiveFailures = 0;
    this.consecutiveSuccesses = 0;

    // Sliding health window: one bucket per recorded outcome.
    this.window = []; // [{ ok, fail }]
    this.windowOk = 0;
    this.windowFail = 0;

    this.openedAt = -1;
    this.recoveringUntil = -1; // tick at which ramp-up finishes, -1 = fully restored
  }

  get isRecovering() {
    return this.state === STATE_CLOSED && this.recoveringUntil > this.now;
  }

  /** Effective concurrency cap for the current state. */
  capacity() {
    const { maxConcurrency, halfOpenMaxProbes, rampUpTicks } = this.config;
    if (this.state === STATE_OPEN) return 0;
    if (this.state === STATE_HALF_OPEN) return halfOpenMaxProbes;
    if (this.isRecovering) {
      const elapsed = rampUpTicks - (this.recoveringUntil - this.now);
      const allowed = Math.ceil(((elapsed + 1) / (rampUpTicks + 1)) * maxConcurrency);
      return Math.max(halfOpenMaxProbes, Math.min(allowed, maxConcurrency));
    }
    return maxConcurrency;
  }

  /** Try to admit one request. Returns true when a slot was granted. */
  tryAcquire() {
    if (this.inFlight >= this.capacity()) return false;
    this.inFlight += 1;
    return true;
  }

  /** Record the outcome of a previously acquired request. */
  release(outcome) {
    if (this.inFlight > 0) this.inFlight -= 1;
    if (outcome === 'success') this._onSuccess();
    else this._onFailure();
  }

  _onSuccess() {
    this._record(true);
    this.consecutiveFailures = 0;
    this.consecutiveSuccesses += 1;
    if (
      this.state === STATE_HALF_OPEN &&
      this.consecutiveSuccesses >= this.config.halfOpenSuccessThreshold
    ) {
      this._close();
    }
  }

  _onFailure() {
    this._record(false);
    this.consecutiveFailures += 1;
    this.consecutiveSuccesses = 0;
    if (this.state === STATE_HALF_OPEN) {
      // A single failed probe re-opens the breaker.
      this._open();
      return;
    }
    if (this.state === STATE_CLOSED && this._shouldTrip()) {
      this._open();
    }
  }

  _shouldTrip() {
    if (this.consecutiveFailures >= this.config.failureThreshold) return true;
    const total = this.windowOk + this.windowFail;
    return (
      total >= this.config.windowMinRequests &&
      this.windowFail / total >= this.config.errorRateThreshold
    );
  }

  _open() {
    this.state = STATE_OPEN;
    this.openedAt = this.now;
    this.consecutiveSuccesses = 0;
    this.recoveringUntil = -1;
  }

  _close() {
    this.state = STATE_CLOSED;
    this.consecutiveFailures = 0;
    this.consecutiveSuccesses = 0;
    // Gradual recovery: ramp traffic back up instead of a full reopen.
    this.recoveringUntil = this.now + this.config.rampUpTicks;
  }

  _record(ok) {
    this.window.push({ ok: ok ? 1 : 0, fail: ok ? 0 : 1 });
    if (ok) this.windowOk += 1;
    else this.windowFail += 1;
    while (this.window.length > this.config.windowSize) {
      const old = this.window.shift();
      this.windowOk -= old.ok;
      this.windowFail -= old.fail;
    }
  }

  /** Advance logical time by one tick. */
  tick() {
    this.now += 1;
    if (
      this.state === STATE_OPEN &&
      this.now - this.openedAt >= this.config.openDuration
    ) {
      this.state = STATE_HALF_OPEN;
      this.consecutiveSuccesses = 0;
    }
  }

  stats() {
    const total = this.windowOk + this.windowFail;
    return {
      id: this.id,
      state: this.state,
      inFlight: this.inFlight,
      capacity: this.capacity(),
      consecutiveFailures: this.consecutiveFailures,
      consecutiveSuccesses: this.consecutiveSuccesses,
      windowRequests: total,
      windowErrorRate: total === 0 ? 0 : this.windowFail / total,
      recovering: this.isRecovering,
    };
  }
}

module.exports = {
  CircuitBreaker,
  STATE_CLOSED,
  STATE_OPEN,
  STATE_HALF_OPEN,
};
