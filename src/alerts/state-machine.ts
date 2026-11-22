/**
 * Pure alert state machine - no I/O, fully unit testable.
 *
 * Hysteresis: a rule moves OK -> FIRING only after `requiredBreaches`
 * consecutive breaching evaluations, and FIRING -> OK only after
 * `recoveryEvaluations` consecutive non-breaching evaluations. A transition is
 * reported exactly once, so a metric hovering around its threshold cannot flap.
 */

export type AlertStateName = 'OK' | 'FIRING';
export type Transition = 'FIRING' | 'RESOLVED' | null;

export interface AlertState {
  state: AlertStateName;
  /** Consecutive breaching evaluations (while OK), or total while FIRING. */
  breachCount: number;
  /** Consecutive non-breaching evaluations while FIRING. */
  okCount: number;
}

export interface Hysteresis {
  requiredBreaches: number;
  recoveryEvaluations: number;
}

export const INITIAL_STATE: AlertState = { state: 'OK', breachCount: 0, okCount: 0 };

export function isBreached(
  operator: '>' | '<',
  observed: number,
  threshold: number,
): boolean {
  return operator === '>' ? observed > threshold : observed < threshold;
}

export function nextState(
  prev: AlertState,
  breached: boolean,
  h: Hysteresis,
): { next: AlertState; transition: Transition } {
  if (prev.state === 'OK') {
    if (!breached) {
      return { next: { state: 'OK', breachCount: 0, okCount: 0 }, transition: null };
    }
    const breachCount = prev.breachCount + 1;
    if (breachCount >= h.requiredBreaches) {
      return { next: { state: 'FIRING', breachCount, okCount: 0 }, transition: 'FIRING' };
    }
    return { next: { state: 'OK', breachCount, okCount: 0 }, transition: null };
  }

  // FIRING
  if (breached) {
    return {
      next: { state: 'FIRING', breachCount: prev.breachCount + 1, okCount: 0 },
      transition: null,
    };
  }
  const okCount = prev.okCount + 1;
  if (okCount >= h.recoveryEvaluations) {
    return { next: { state: 'OK', breachCount: 0, okCount: 0 }, transition: 'RESOLVED' };
  }
  return { next: { state: 'FIRING', breachCount: prev.breachCount, okCount }, transition: null };
}
