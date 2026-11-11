import { describe, it, expect } from 'vitest';
import { escCopyText } from '../src/ingest/copy-format.js';

describe('escCopyText', () => {
  it('leaves plain text unchanged', () => {
    expect(escCopyText('/api/login')).toBe('/api/login');
  });

  it('escapes tabs', () => {
    expect(escCopyText('a\tb')).toBe('a\\tb');
  });

  it('escapes newlines and carriage returns', () => {
    expect(escCopyText('a\nb')).toBe('a\\nb');
    expect(escCopyText('a\rb')).toBe('a\\rb');
  });

  it('escapes backslashes', () => {
    expect(escCopyText('a\\b')).toBe('a\\\\b');
  });

  it('escapes backslash before other sequences so escapes are not double-escaped', () => {
    // A literal backslash-t should become \\t (escaped backslash + literal t),
    // not \t (which COPY would read as a tab).
    expect(escCopyText('a\\tb')).toBe('a\\\\tb');
  });

  it('escapes a mix of special characters', () => {
    expect(escCopyText('x\t\n\\y')).toBe('x\\t\\n\\\\y');
  });
});
