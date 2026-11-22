import { describe, it, expect } from 'vitest';
import { INITIAL_STATE, isBreached, nextState, type AlertState } from '../src/alerts/state-machine.js';
import { backoffDelayMs } from '../src/alerts/backoff.js';

const H = (requiredBreaches: number, recoveryEvaluations: number) => ({
  requiredBreaches,
  recoveryEvaluations,
});

/** Feed a sequence of breach booleans; return the transitions in order. */
function run(seq: boolean[], h = H(3, 2)): { transitions: (string | null)[]; last: AlertState } {
  let st = INITIAL_STATE;
  const transitions: (string | null)[] = [];
  for (const b of seq) {
    const r = nextState(st, b, h);
    transitions.push(r.transition);
    st = r.next;
  }
  return { transitions, last: st };
}

describe('isBreached', () => {
  it('compares strictly in the rule direction', () => {
    expect(isBreached('>', 5.1, 5)).toBe(true);
    expect(isBreached('>', 5, 5)).toBe(false);
    expect(isBreached('<', 4.9, 5)).toBe(true);
    expect(isBreached('<', 5, 5)).toBe(false);
  });
});

describe('alert state machine', () => {
  it('fires on the first breach with default hysteresis (1, 1)', () => {
    const { transitions } = run([true, true, false], H(1, 1));
    expect(transitions).toEqual(['FIRING', null, 'RESOLVED']);
  });

  it('does not fire before required_consecutive_breaches', () => {
    const { transitions, last } = run([true, true], H(3, 1));
    expect(transitions).toEqual([null, null]);
    expect(last.state).toBe('OK');
    expect(last.breachCount).toBe(2);
  });

  it('fires exactly once on the Nth consecutive breach, then stays quiet', () => {
    const { transitions } = run([true, true, true, true, true], H(3, 1));
    expect(transitions).toEqual([null, null, 'FIRING', null, null]);
  });

  it('a clean evaluation resets the breach streak (no flapping)', () => {
    const { transitions } = run([true, true, false, true, true, false, true], H(3, 1));
    expect(transitions.every((t) => t === null)).toBe(true);
  });

  it('resolves only after recovery_evaluations consecutive clean evaluations', () => {
    const { transitions, last } = run([true, true, true, false, false], H(3, 3));
    expect(transitions).toEqual([null, null, 'FIRING', null, null]);
    expect(last.state).toBe('FIRING');
    const done = run([true, true, true, false, false, false], H(3, 3));
    expect(done.transitions[5]).toBe('RESOLVED');
    expect(done.last).toEqual(INITIAL_STATE);
  });

  it('a breach during recovery keeps it FIRING and resets the recovery streak', () => {
    const { transitions, last } = run([true, true, true, false, false, true, false, false], H(3, 3));
    expect(transitions.filter((t) => t === 'RESOLVED')).toHaveLength(0);
    expect(last.state).toBe('FIRING');
    expect(last.okCount).toBe(2);
  });

  it('can fire again after resolving', () => {
    const { transitions } = run([true, false, true, false], H(1, 1));
    expect(transitions).toEqual(['FIRING', 'RESOLVED', 'FIRING', 'RESOLVED']);
  });
});

describe('backoffDelayMs', () => {
  it('grows exponentially within [ceiling/2, ceiling)', () => {
    const lo = () => 0;
    const hi = () => 0.999999;
    expect(backoffDelayMs(1, lo, 1000)).toBe(500);
    expect(backoffDelayMs(2, lo, 1000)).toBe(1000);
    expect(backoffDelayMs(3, lo, 1000)).toBe(2000);
    expect(backoffDelayMs(3, hi, 1000)).toBeLessThan(4000);
    expect(backoffDelayMs(3, hi, 1000)).toBeGreaterThanOrEqual(3999);
  });

  it('is capped', () => {
    expect(backoffDelayMs(40, () => 0, 1000, 60_000)).toBe(30_000);
    expect(backoffDelayMs(40, () => 0.999999, 1000, 60_000)).toBeLessThan(60_000);
  });

  it('jitter spreads values across the window', () => {
    const vals = new Set(Array.from({ length: 50 }, () => backoffDelayMs(5, Math.random, 1000)));
    expect(vals.size).toBeGreaterThan(10);
  });

  it('treats attempt < 1 like the first attempt', () => {
    expect(backoffDelayMs(0, () => 0, 1000)).toBe(500);
  });
});
