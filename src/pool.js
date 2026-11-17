'use strict';

const { CircuitBreaker } = require('./circuit-breaker');

class PoolExhaustedError extends Error {
  constructor(message = 'no endpoint available: pool exhausted') {
    super(message);
    this.name = 'PoolExhaustedError';
    this.code = 'POOL_EXHAUSTED';
  }
}

class RetryBudgetExhaustedError extends Error {
  constructor(message = 'retry budget exhausted') {
    super(message);
    this.name = 'RetryBudgetExhaustedError';
    this.code = 'RETRY_BUDGET_EXHAUSTED';
  }
}

/**
 * A load-balancing pool of circuit-breaker-protected endpoints with a
 * shared retry budget. Time advances only via tick().
 */
class MeshPool {
  constructor(options = {}) {
    const {
      retryBudgetPerTick = 4, // retry tokens refilled every tick
      maxRetries = 3,         // per-request retry cap
      ...breakerOptions
    } = options;
    this.breakerOptions = breakerOptions;
    this.retryBudgetPerTick = retryBudgetPerTick;
    this.retryTokens = retryBudgetPerTick;
    this.maxRetries = maxRetries;
    this.now = 0;
    this.endpoints = new Map();
  }

  addEndpoint(id, overrides = {}) {
    const breaker = new CircuitBreaker(id, { ...this.breakerOptions, ...overrides });
    this.endpoints.set(id, breaker);
    return breaker;
  }

  getEndpoint(id) {
    return this.endpoints.get(id);
  }

  /** Advance logical time: tick every breaker and refill the retry budget. */
  tick() {
    this.now += 1;
    this.retryTokens = this.retryBudgetPerTick;
    for (const breaker of this.endpoints.values()) breaker.tick();
  }

  /**
   * Pick the least-loaded endpoint that currently has capacity.
   * Returns null when every endpoint is open or saturated.
   */
  route() {
    let best = null;
    let bestLoad = Infinity;
    for (const breaker of this.endpoints.values()) {
      const capacity = breaker.capacity();
      if (capacity <= 0 || breaker.inFlight >= capacity) continue;
      const load = breaker.inFlight / capacity;
      if (load < bestLoad) {
        bestLoad = load;
        best = breaker;
      }
    }
    return best;
  }

  /**
   * Execute fn(endpointId) against the pool with retries.
   *
   * fn must return a value on success or throw on failure. The first
   * attempt is always allowed when an endpoint has capacity; every retry
   * consumes one token from the shared retry budget. When no endpoint can
   * accept traffic the call fails fast with PoolExhaustedError instead of
   * retrying forever.
   */
  execute(fn, { maxRetries = this.maxRetries } = {}) {
    let lastError = null;
    for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
      if (attempt > 0) {
        if (this.retryTokens <= 0) {
          const err = new RetryBudgetExhaustedError();
          err.cause = lastError;
          throw err;
        }
        this.retryTokens -= 1;
      }
      const breaker = this.route();
      if (!breaker) {
        const err = new PoolExhaustedError();
        err.cause = lastError;
        throw err;
      }
      breaker.tryAcquire();
      try {
        const result = fn(breaker.id);
        breaker.release('success');
        return result;
      } catch (err) {
        breaker.release('failure');
        lastError = err;
      }
    }
    throw lastError;
  }

  stats() {
    return {
      now: this.now,
      retryTokens: this.retryTokens,
      endpoints: [...this.endpoints.values()].map((b) => b.stats()),
    };
  }
}

module.exports = { MeshPool, PoolExhaustedError, RetryBudgetExhaustedError };
