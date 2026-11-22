/**
 * Encoder for the PostgreSQL binary COPY format. Pure, no I/O.
 * Layout: 11-byte signature, int32 flags, int32 header-extension length, then
 * per row an int16 field count and per field an int32 byte length (-1 = NULL)
 * followed by the value, and finally an int16 -1 trailer.
 */

export type ColumnType = 'int2' | 'int4' | 'text' | 'timestamptz';
export type CopyValue = string | number | Date | null;

const SIGNATURE = Buffer.from('PGCOPY\n\xff\r\n\0', 'latin1');
// Postgres timestamps count microseconds from 2000-01-01T00:00:00Z.
const PG_EPOCH_MS = 946_684_800_000;

function encodeValue(type: ColumnType, v: Exclude<CopyValue, null>): Buffer {
  switch (type) {
    case 'int2': {
      if (typeof v !== 'number' || !Number.isInteger(v) || v < -32768 || v > 32767) {
        throw new RangeError(`invalid int2 value: ${String(v)}`);
      }
      const b = Buffer.allocUnsafe(2);
      b.writeInt16BE(v);
      return b;
    }
    case 'int4': {
      if (typeof v !== 'number' || !Number.isInteger(v) || v < -2147483648 || v > 2147483647) {
        throw new RangeError(`invalid int4 value: ${String(v)}`);
      }
      const b = Buffer.allocUnsafe(4);
      b.writeInt32BE(v);
      return b;
    }
    case 'text':
      if (typeof v !== 'string') throw new TypeError(`invalid text value: ${String(v)}`);
      return Buffer.from(v, 'utf8');
    case 'timestamptz': {
      const ms = v instanceof Date ? v.getTime() : NaN;
      if (Number.isNaN(ms)) throw new TypeError('invalid timestamptz value');
      const b = Buffer.allocUnsafe(8);
      b.writeBigInt64BE(BigInt(ms - PG_EPOCH_MS) * 1000n);
      return b;
    }
  }
}

export function encodeBinaryCopy(types: readonly ColumnType[], rows: readonly CopyValue[][]): Buffer {
  const header = Buffer.alloc(8); // flags + extension length, both zero
  const parts: Buffer[] = [SIGNATURE, header];

  for (const row of rows) {
    if (row.length !== types.length) {
      throw new RangeError(`row has ${row.length} values, expected ${types.length}`);
    }
    const count = Buffer.allocUnsafe(2);
    count.writeInt16BE(types.length);
    parts.push(count);
    types.forEach((type, i) => {
      const v = row[i];
      const len = Buffer.allocUnsafe(4);
      if (v === null) {
        len.writeInt32BE(-1);
        parts.push(len);
        return;
      }
      const data = encodeValue(type, v);
      len.writeInt32BE(data.length);
      parts.push(len, data);
    });
  }

  const trailer = Buffer.allocUnsafe(2);
  trailer.writeInt16BE(-1);
  parts.push(trailer);
  return Buffer.concat(parts);
}
