import { describe, it, expect } from 'vitest';
import type { Pool } from 'pg';
import { PoolExhaustedError } from '../src/db/pool.js';
import {
  ReplicaRouter,
  isReplicaFailure,
  lagSecondsFromProbe,
  type RouterOptions,
} from '../src/db/router.js';

// Distinct sentinel objects stand in for the pools; the router only compares them.
const primary = { name: 'primary' } as unknown as Pool;
const replica = { name: 'replica' } as unknown as Pool;

function make(samples: Array<number | Error>, over: Partial<RouterOptions> = {}) {
  let i = 0;
  const logs: string[] = [];
  const router = new ReplicaRouter({
    primary,
    replica,
    sample: async () => {
      const s = samples[Math.min(i++, samples.length - 1)];
      if (s instanceof Error) throw s;
      return s;
    },
    maxLagMs: 2000,
    intervalMs: 1000,
    recoveryProbes: 3,
    probeTimeoutMs: 200,
    log: (m) => logs.push(m),
    ...over,
  });
  return { router, logs };
}

describe('lagSecondsFromProbe', () => {
  const base = { in_recovery: true, streaming: true, caught_up: false, since_replay: 0.5 };
  it('reports measured lag while behind', () => {
    expect(lagSecondsFromProbe(base)).toBe(0.5);
  });
  it('treats a caught-up replica as zero lag even if the last replay is old', () => {
    expect(lagSecondsFromProbe({ ...base, caught_up: true, since_replay: 3600 })).toBe(0);
  });
  it('is unusable when not a standby or not streaming', () => {
    expect(lagSecondsFromProbe({ ...base, in_recovery: false })).toBe(Infinity);
    expect(lagSecondsFromProbe({ ...base, streaming: false })).toBe(Infinity);
  });
  it('is unusable when behind with no replay timestamp to measure from', () => {
    expect(lagSecondsFromProbe({ ...base, since_replay: null })).toBe(Infinity);
  });
  it('never returns a negative lag (clock skew)', () => {
    expect(lagSecondsFromProbe({ ...base, since_replay: -0.2 })).toBe(0);
  });
});

describe('ReplicaRouter', () => {
  it('starts on the primary until the first successful probe', () => {
    const { router } = make([0.1]);
    expect(router.getReadPool()).toBe(primary);
  });

  it('uses the replica when lag is within the limit (<= 2s)', async () => {
    const { router } = make([0.1, 2.0]);
    await router.probeOnce();
    expect(router.getReadPool()).toBe(replica);
    await router.probeOnce(); // exactly at the limit is still healthy
    expect(router.getReadPool()).toBe(replica);
    expect(router.status().target).toBe('replica');
  });

  it('fails over to the primary immediately when lag exceeds 2s', async () => {
    const { router, logs } = make([0.1, 2.5]);
    await router.probeOnce();
    await router.probeOnce();
    expect(router.getReadPool()).toBe(primary);
    const s = router.status();
    expect(s.target).toBe('primary');
    expect(s.lag_ms).toBe(2500);
    expect(s.failovers).toBe(1);
    expect(s.reason).toMatch(/lag 2500ms > 2000ms/);
    expect(logs.some((l) => /failing over/.test(l))).toBe(true);
  });

  it('fails over when the probe errors, and when it hangs', async () => {
    const a = make([0.1, new Error('ECONNREFUSED')]);
    await a.router.probeOnce();
    await a.router.probeOnce();
    expect(a.router.getReadPool()).toBe(primary);
    expect(a.router.status().reason).toMatch(/ECONNREFUSED/);

    let call = 0;
    const b = make([], {
      sample: () => (++call === 1 ? Promise.resolve(0) : new Promise<number>(() => {})),
      probeTimeoutMs: 20,
    });
    await b.router.probeOnce();
    expect(b.router.getReadPool()).toBe(replica);
    await b.router.probeOnce();
    expect(b.router.getReadPool()).toBe(primary);
    expect(b.router.status().reason).toMatch(/timed out/);
  });

  it('fails over when the replica is not streaming (Infinity)', async () => {
    const { router } = make([0.1, Infinity]);
    await router.probeOnce();
    await router.probeOnce();
    expect(router.getReadPool()).toBe(primary);
    expect(router.status().lag_ms).toBeNull();
    expect(router.status().reason).toMatch(/not streaming/);
  });

  it('returns to the replica only after N consecutive good probes', async () => {
    const { router, logs } = make([0.1, 5, 0.1, 0.1, 5, 0.1, 0.1, 0.1]);
    await router.probeOnce(); // healthy
    await router.probeOnce(); // lag 5s -> primary
    expect(router.getReadPool()).toBe(primary);
    await router.probeOnce(); // good 1
    await router.probeOnce(); // good 2
    expect(router.getReadPool()).toBe(primary);
    await router.probeOnce(); // bad again resets the streak
    await router.probeOnce(); // good 1
    await router.probeOnce(); // good 2
    expect(router.getReadPool()).toBe(primary);
    await router.probeOnce(); // good 3
    expect(router.getReadPool()).toBe(replica);
    expect(router.status().failovers).toBe(1); // a bad probe while already on primary is not a new failover
    expect(logs.filter((l) => /replica healthy/.test(l))).toHaveLength(2);
  });

  it('shares one in-flight probe between concurrent callers', async () => {
    let calls = 0;
    const { router } = make([], {
      sample: async () => {
        calls += 1;
        await new Promise((r) => setTimeout(r, 20));
        return 0;
      },
    });
    await Promise.all([router.probeOnce(), router.probeOnce(), router.probeOnce()]);
    expect(calls).toBe(1);
  });

  it('with no replica configured always uses the primary and never probes', async () => {
    let calls = 0;
    const { router } = make([], {
      replica: null,
      sample: async () => {
        calls += 1;
        return 0;
      },
    });
    await router.probeOnce();
    await router.start();
    expect(router.getReadPool()).toBe(primary);
    expect(router.status()).toMatchObject({ target: 'primary', replica_configured: false });
    expect(calls).toBe(0);
    router.stop();
  });

  it('start() probes once up front, then keeps probing until stop()', async () => {
    let calls = 0;
    const { router } = make([], {
      intervalMs: 15,
      sample: async () => {
        calls += 1;
        return 0;
      },
    });
    await router.start();
    expect(router.getReadPool()).toBe(replica);
    await new Promise((r) => setTimeout(r, 70));
    router.stop();
    const after = calls;
    expect(after).toBeGreaterThan(2);
    await new Promise((r) => setTimeout(r, 50));
    expect(calls).toBe(after);
  });
});

describe('isReplicaFailure', () => {
  it('flags connection, shutdown and recovery-conflict errors', () => {
    for (const code of ['ECONNREFUSED', 'ECONNRESET', '08006', '08001', '57P01', '57P03', '40001']) {
      expect(isReplicaFailure(Object.assign(new Error('x'), { code }))).toBe(true);
    }
    expect(isReplicaFailure(new PoolExhaustedError('busy'))).toBe(true);
  });
  it('does not flag query errors that would fail identically on the primary', () => {
    for (const code of ['42P01', '42601', '22P02', '23505']) {
      expect(isReplicaFailure(Object.assign(new Error('x'), { code }))).toBe(false);
    }
    expect(isReplicaFailure(new Error('plain'))).toBe(false);
    expect(isReplicaFailure(null)).toBe(false);
  });
});
