import process from 'node:process';
import { parsePositiveInt } from '../middleware/config.js';

/** Partition lifecycle settings, read from the environment at call time. */

/** Daily partitions are pre-created this many days past today. */
export function aheadDays(): number {
  return parsePositiveInt(process.env.PARTITION_AHEAD_DAYS, 7);
}

/** Partitions that ended more than this many days ago are archived and dropped. */
export function retentionDays(): number {
  return parsePositiveInt(process.env.PARTITION_RETENTION_DAYS, 90);
}
