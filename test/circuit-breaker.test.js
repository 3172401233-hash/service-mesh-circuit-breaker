'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  CircuitBreaker,
  MeshPool,
  PoolExhaustedError,
  RetryBudgetExhaustedError,
} = require('../index');

const CONFIG = {
  failureThreshold: 3,
  windowSize: 10,
  windowMinRequests: 4,
  errorRateThreshold: 0.5,
  openDuration: 5,
  halfOpenMaxProbes: 2,
  halfOpenSuccessThreshold: 2,
  rampUpTicks: 3,
  maxConcurrency: 8,
};

function fail(breaker, times = 1) {
  for (let i = 0; i < times; i += 1) {
    assert.equal(breaker.tryAcquire(), true, 'expected a slot before failing');
    breaker.release('failure');
  }
}

test('fast failure: consecutive errors trip the breaker within one tick', () => {
  const b = new CircuitBreaker('ep', CONFIG);
  fail(b, 2);
  assert.equal(b.state, 'closed');
  fail(b, 1);
  assert.equal(b.state, 'open');
  assert.equal(b.tryAcquire(), false, 'open breaker rejects traffic');
  assert.equal(b.capacity(), 0);
});

test('slow failure: elevated error rate over the health window trips the breaker', () => {
  const b = new CircuitBreaker('ep', CONFIG);
  // Interleave successes so the consecutive-error counter never reaches 3.
  const outcomes = ['failure', 'success', 'failure', 'success', 'failure'];
  for (const outcome of outcomes) {
    assert.equal(b.tryAcquire(), true);
    b.release(outcome);
  }
  // 3 failures out of 5 requests = 60% error rate, above the 50% threshold.
  assert.equal(b.state, 'open');
  assert.equal(b.stats().windowErrorRate, 0.6);
});

test('breaker stays closed below the error-rate threshold', () => {
  const b = new CircuitBreaker('ep', CONFIG);
  const outcomes = ['failure', 'success', 'success', 'success', 'failure', 'success'];
  for (const outcome of outcomes) {
    b.tryAcquire();
    b.release(outcome);
  }
  assert.equal(b.state, 'closed');
});

test('open breaker transitions to half-open only after openDuration ticks', () => {
  const b = new CircuitBreaker('ep', CONFIG);
  fail(b, 3);
  assert.equal(b.state, 'open');
  for (let i = 0; i < 4; i += 1) b.tick();
  assert.equal(b.state, 'open', 'still open before the cooldown elapses');
  b.tick();
  assert.equal(b.state, 'half-open');
});

test('half-open race: only a bounded number of probes are admitted', () => {
  const b = new CircuitBreaker('ep', CONFIG);
  fail(b, 3);
  for (let i = 0; i < 5; i += 1) b.tick();
  assert.equal(b.state, 'half-open');

  assert.equal(b.tryAcquire(), true, 'probe 1 admitted');
  assert.equal(b.tryAcquire(), true, 'probe 2 admitted');
  assert.equal(b.tryAcquire(), false, 'third concurrent probe rejected');

  // A failed probe immediately re-opens the breaker, racing probes lose.
  b.release('failure');
  assert.equal(b.state, 'open');
  assert.equal(b.tryAcquire(), false);
  b.release('failure'); // release the outstanding probe
});

test('half-open probes succeed: breaker closes and ramps traffic gradually', () => {
  const b = new CircuitBreaker('ep', CONFIG);
  fail(b, 3);
  for (let i = 0; i < 5; i += 1) b.tick();

  b.tryAcquire();
  b.release('success');
  assert.equal(b.state, 'half-open', 'one success is not enough');
  b.tryAcquire();
  b.release('success');
  assert.equal(b.state, 'closed');
  assert.equal(b.isRecovering, true, 'recovery ramps up instead of full reopen');

  // Ramp-up: capacity grows tick by tick until fully restored.
  const seen = [b.capacity()];
  for (let i = 0; i < 3; i += 1) {
    b.tick();
    seen.push(b.capacity());
  }
  assert.deepEqual(seen, [2, 4, 6, 8]);
  assert.equal(b.isRecovering, false);
  assert.equal(b.capacity(), CONFIG.maxConcurrency);
});

test('concurrency limit is enforced while closed', () => {
  const b = new CircuitBreaker('ep', CONFIG);
  for (let i = 0; i < CONFIG.maxConcurrency; i += 1) {
    assert.equal(b.tryAcquire(), true);
  }
  assert.equal(b.tryAcquire(), false, 'in-flight cap reached');
  b.release('success');
  assert.equal(b.tryAcquire(), true, 'slot freed after release');
});

test('pool: broken endpoint is ejected and retries land on a healthy peer', () => {
  const pool = new MeshPool({ ...CONFIG, failureThreshold: 1, retryBudgetPerTick: 2 });
  pool.addEndpoint('a');
  pool.addEndpoint('b');

  const result = pool.execute((id) => {
    if (id === 'a') throw new Error('boom');
    return `ok:${id}`;
  });
  assert.equal(result, 'ok:b');
  assert.equal(pool.getEndpoint('a').state, 'open');
  assert.equal(pool.getEndpoint('b').state, 'closed');
  assert.equal(pool.stats().retryTokens, 1, 'one retry token consumed');
});

test('pool: exhaustion fails fast when every endpoint is open', () => {
  const pool = new MeshPool({ ...CONFIG, failureThreshold: 1 });
  pool.addEndpoint('a');
  pool.addEndpoint('b');
  fail(pool.getEndpoint('a'), 1);
  fail(pool.getEndpoint('b'), 1);

  assert.throws(() => pool.execute(() => 'never'), PoolExhaustedError);
  // Still exhausted on the next tick until the cooldown elapses.
  pool.tick();
  assert.throws(() => pool.execute(() => 'never'), PoolExhaustedError);
  // After the cooldown both endpoints become half-open and accept probes.
  for (let i = 0; i < 4; i += 1) pool.tick();
  assert.equal(pool.execute(() => 'recovered'), 'recovered');
});

test('pool: retry budget stops infinite retries and refills each tick', () => {
  const pool = new MeshPool({
    ...CONFIG,
    failureThreshold: 100, // endpoint never opens: isolates the budget
    retryBudgetPerTick: 2,
    maxRetries: 10,
  });
  pool.addEndpoint('a');

  assert.throws(
    () => pool.execute(() => { throw new Error('always fails'); }),
    RetryBudgetExhaustedError,
  );
  assert.equal(pool.stats().retryTokens, 0);

  // Budget is refilled on the next tick, so callers may retry then.
  pool.tick();
  assert.equal(pool.stats().retryTokens, 2);
  assert.equal(pool.execute(() => 'ok'), 'ok');
});

test('pool: per-request maxRetries caps attempts even with budget left', () => {
  const pool = new MeshPool({ ...CONFIG, failureThreshold: 100, retryBudgetPerTick: 10 });
  pool.addEndpoint('a');
  let attempts = 0;
  assert.throws(
    () => pool.execute(() => { attempts += 1; throw new Error('nope'); }, { maxRetries: 2 }),
    /nope/,
  );
  assert.equal(attempts, 3, 'initial attempt plus two retries');
});
