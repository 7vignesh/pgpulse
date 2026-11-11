import { describe, it, expect } from 'vitest';
import { isHttpsUrl } from '../src/alerts/validation.js';

describe('isHttpsUrl', () => {
  it('accepts a plain https URL', () => {
    expect(isHttpsUrl('https://example.com/webhook')).toBe(true);
  });

  it('accepts https regardless of scheme case', () => {
    expect(isHttpsUrl('HTTPS://example.com/webhook')).toBe(true);
  });

  it('rejects http URLs', () => {
    expect(isHttpsUrl('http://example.com/webhook')).toBe(false);
  });

  it('rejects other schemes', () => {
    expect(isHttpsUrl('ftp://example.com/x')).toBe(false);
    expect(isHttpsUrl('file:///etc/passwd')).toBe(false);
  });

  it('rejects URLs with embedded credentials', () => {
    expect(isHttpsUrl('https://user:pass@example.com/webhook')).toBe(false);
    expect(isHttpsUrl('https://user@example.com/webhook')).toBe(false);
  });

  it('rejects non-URL strings', () => {
    expect(isHttpsUrl('not a url')).toBe(false);
    expect(isHttpsUrl('')).toBe(false);
  });
});
