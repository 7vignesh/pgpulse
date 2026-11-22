import { describe, it, expect } from 'vitest';
import {
  archiveKey,
  assertIdent,
  ingestWindow,
  isExpired,
  parseBound,
  quoteIdent,
  utcDay,
} from '../src/partitions/plan.js';
import { toColumns, type ExportRow } from '../src/partitions/export.js';

const d = (s: string): Date => new Date(s);

describe('parseBound', () => {
  it('parses monthly and daily bounds printed in UTC', () => {
    expect(parseBound("FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00')")).toEqual({
      from: d('2026-10-01T00:00:00Z'),
      to: d('2026-11-01T00:00:00Z'),
      isDefault: false,
    });
    expect(parseBound("FOR VALUES FROM ('2026-11-25 00:00:00+00') TO ('2026-11-26 00:00:00+00')").to).toEqual(
      d('2026-11-26T00:00:00Z'),
    );
  });
  it('accepts a date-only bound and recognises DEFAULT', () => {
    expect(parseBound("FOR VALUES FROM ('2026-11-25+00') TO ('2026-11-26+00')").from).toEqual(d('2026-11-25T00:00:00Z'));
    expect(parseBound('DEFAULT')).toEqual({ from: null, to: null, isDefault: true });
  });
  it('rejects bounds in another time zone instead of misreading them', () => {
    expect(() => parseBound("FOR VALUES FROM ('2026-11-25 00:00:00+05:30') TO ('2026-11-26 00:00:00+05:30')")).toThrow(/UTC/);
    expect(() => parseBound('FOR VALUES IN (1)')).toThrow(/unsupported/);
  });
});

describe('isExpired', () => {
  const now = d('2026-11-25T13:45:00Z');
  const p = (to: string) => ({ to: d(to), isDefault: false });

  it('expires partitions that ended on or before the retention cutoff', () => {
    // retention 90d: cutoff = 2026-11-25 - 90d = 2026-08-27
    expect(isExpired(p('2026-08-27T00:00:00Z'), now, 90)).toBe(true);
    expect(isExpired(p('2026-08-26T00:00:00Z'), now, 90)).toBe(true);
  });
  it('keeps partitions that end after the cutoff, including ones straddling it', () => {
    expect(isExpired(p('2026-08-28T00:00:00Z'), now, 90)).toBe(false);
    expect(isExpired(p('2026-11-26T00:00:00Z'), now, 90)).toBe(false);
  });
  it('never expires the DEFAULT partition', () => {
    expect(isExpired({ to: null, isDefault: true }, now, 0)).toBe(false);
  });
});

describe('ingestWindow', () => {
  it('spans the retention cutoff to the end of the last pre-created day', () => {
    const w = ingestWindow(d('2026-11-25T13:45:00Z'), 90, 7);
    expect(w.min).toEqual(d('2026-08-27T00:00:00Z'));
    expect(w.max).toEqual(d('2026-12-03T00:00:00Z')); // exclusive: day +7 is the last day
  });
});

describe('archiveKey / identifiers', () => {
  it('lays objects out by partition day', () => {
    expect(archiveKey('archive', 'events', 'events_20260801', d('2026-08-01T00:00:00Z'))).toBe(
      'archive/events/dt=2026-08-01/events_20260801.parquet',
    );
    expect(archiveKey('/a/b/', 'events', 'events_2026_08', d('2026-08-01T00:00:00Z'))).toBe(
      'a/b/events/dt=2026-08-01/events_2026_08.parquet',
    );
    expect(archiveKey('', 'events', 'events_x', d('2026-08-01T00:00:00Z'))).toBe('events/dt=2026-08-01/events_x.parquet');
  });
  it('refuses identifiers that could inject SQL', () => {
    expect(quoteIdent('events_20260801')).toBe('"events_20260801"');
    for (const bad of ['events; drop table x', 'Events', 'a"b', '', 'a b', '1abc', 'x'.repeat(64)]) {
      expect(() => assertIdent(bad)).toThrow(/unsafe/);
    }
  });
  it('utcDay truncates to UTC midnight', () => {
    expect(utcDay(d('2026-11-25T23:59:59.999Z'))).toEqual(d('2026-11-25T00:00:00Z'));
  });
});

describe('toColumns', () => {
  const row: ExportRow = {
    id: '9007199254740993', // > 2^53: must not be rounded through Number
    tenant_id: '0b5f4d7e-1111-4222-8333-444455556666',
    endpoint: '/a',
    method: 'GET',
    status_code: 200,
    latency_ms: 12,
    user_agent: null,
    ip_address: '10.0.0.1',
    metadata: '{"a": 1}',
    ingested_micros: '1763841234123457',
  };
  it('keeps int8 precision and microsecond timestamps exact', () => {
    const cols = toColumns([row]);
    expect(cols.find((c) => c.name === 'id')!.data[0]).toBe(9007199254740993n);
    expect(cols.find((c) => c.name === 'ingested_at')!.data[0]).toBe(1763841234123457n);
    expect(cols.find((c) => c.name === 'user_agent')!.data[0]).toBeNull();
  });
  it('emits one column per events column, in schema order', () => {
    expect(toColumns([row]).map((c) => c.name)).toEqual([
      'id', 'tenant_id', 'endpoint', 'method', 'status_code', 'latency_ms',
      'user_agent', 'ip_address', 'metadata', 'ingested_at',
    ]);
  });
});
