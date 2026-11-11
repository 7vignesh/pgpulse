import { describe, it, expect } from 'vitest';
import { parsePositiveInt } from '../src/middleware/config.js';

describe('parsePositiveInt', () => {
  it('parses a valid positive integer', () => {
    expect(parsePositiveInt('500', 1000)).toBe(500);
  });

  it('falls back for undefined / empty', () => {
    expect(parsePositiveInt(undefined, 1000)).toBe(1000);
    expect(parsePositiveInt('', 1000)).toBe(1000);
  });

  it('falls back for non-numeric strings', () => {
    expect(parsePositiveInt('abc', 1000)).toBe(1000);
  });

  it('falls back for fractional values', () => {
    expect(parsePositiveInt('10.5', 1000)).toBe(1000);
  });

  it('falls back for zero and negatives', () => {
    expect(parsePositiveInt('0', 1000)).toBe(1000);
    expect(parsePositiveInt('-5', 1000)).toBe(1000);
  });

  it('falls back for Infinity / NaN spellings', () => {
    expect(parsePositiveInt('Infinity', 1000)).toBe(1000);
    expect(parsePositiveInt('NaN', 1000)).toBe(1000);
  });
});
