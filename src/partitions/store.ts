import { createReadStream } from 'node:fs';
import process from 'node:process';
import {
  GetObjectCommand,
  HeadObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { parquetMetadataAsync } from 'hyparquet';

/**
 * Object storage used for cold archives. A small interface so the manager can
 * be tested with failing stores, with one S3 implementation that works against
 * AWS S3 and MinIO.
 */

export interface StoredObject {
  size: number;
  meta: Record<string, string>;
}

export interface ObjectStore {
  /** Upload a local file. Overwrites an existing object (idempotent retries). */
  put(key: string, filePath: string, meta: Record<string, string>): Promise<void>;
  /** null when the object does not exist. */
  head(key: string): Promise<StoredObject | null>;
  /** Bytes [start, end) of the object. */
  readRange(key: string, start: number, end: number): Promise<Uint8Array>;
}

export class S3Store implements ObjectStore {
  constructor(
    private readonly client: S3Client,
    private readonly bucket: string,
  ) {}

  async put(key: string, filePath: string, meta: Record<string, string>): Promise<void> {
    // lib-storage switches to multipart for large files and streams from disk.
    const upload = new Upload({
      client: this.client,
      params: {
        Bucket: this.bucket,
        Key: key,
        Body: createReadStream(filePath),
        ContentType: 'application/vnd.apache.parquet',
        Metadata: meta,
      },
    });
    await upload.done();
  }

  async head(key: string): Promise<StoredObject | null> {
    try {
      const r = await this.client.send(new HeadObjectCommand({ Bucket: this.bucket, Key: key }));
      return { size: Number(r.ContentLength ?? 0), meta: r.Metadata ?? {} };
    } catch (err) {
      const e = err as { name?: string; $metadata?: { httpStatusCode?: number } };
      if (e.name === 'NotFound' || e.name === 'NoSuchKey' || e.$metadata?.httpStatusCode === 404) {
        return null;
      }
      throw err;
    }
  }

  async readRange(key: string, start: number, end: number): Promise<Uint8Array> {
    const r = await this.client.send(
      new GetObjectCommand({ Bucket: this.bucket, Key: key, Range: `bytes=${start}-${end - 1}` }),
    );
    if (!r.Body) throw new Error(`empty response reading ${key}`);
    return r.Body.transformToByteArray();
  }
}

/**
 * Build the store from S3_* variables, or null when S3_BUCKET is unset
 * (archival disabled). S3_ENDPOINT points at MinIO; leave it unset for AWS.
 */
export function storeFromEnv(env: NodeJS.ProcessEnv = process.env): S3Store | null {
  const bucket = env.S3_BUCKET;
  if (!bucket) return null;
  const endpoint = env.S3_ENDPOINT || undefined;
  const client = new S3Client({
    region: env.S3_REGION || 'us-east-1',
    endpoint,
    // MinIO needs path-style addressing (bucket in the path, not the hostname).
    forcePathStyle: env.S3_FORCE_PATH_STYLE ? env.S3_FORCE_PATH_STYLE === 'true' : endpoint !== undefined,
    credentials:
      env.S3_ACCESS_KEY && env.S3_SECRET_KEY
        ? { accessKeyId: env.S3_ACCESS_KEY, secretAccessKey: env.S3_SECRET_KEY }
        : undefined, // fall back to the default AWS credential chain
  });
  return new S3Store(client, bucket);
}

/**
 * Row count recorded in the Parquet footer of an uploaded object. Reads only
 * the footer with ranged GETs, so verifying a large archive does not download
 * it, and proves the stored bytes are a readable Parquet file.
 */
export async function parquetRowCount(store: ObjectStore, key: string): Promise<number> {
  const head = await store.head(key);
  if (!head) throw new Error(`object not found: ${key}`);
  const file = {
    byteLength: head.size,
    slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
      const bytes = await store.readRange(key, start, end ?? head.size);
      return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
    },
  };
  const md = await parquetMetadataAsync(file);
  return Number(md.num_rows);
}
