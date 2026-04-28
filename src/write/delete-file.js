import { writeParquet } from './parquet.js'
import { computeColumnStats } from './stats.js'

/**
 * @import {CompressionCodec} from 'hyparquet'
 * @import {Writer} from 'hyparquet-writer'
 * @import {Schema} from '../../src/types.js'
 */

// Iceberg-reserved field ids for v2 position-delete parquet files
// (spec: "Position Delete Files"). Readers without an iceberg.schema KV
// rely on these ids to bind columns.
const FILE_PATH_FIELD_ID = 2147483546
const POS_FIELD_ID = 2147483545

/** @type {Schema} */
const POSITION_DELETE_SCHEMA = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: FILE_PATH_FIELD_ID, name: 'file_path', required: true, type: 'string' },
    { id: POS_FIELD_ID, name: 'pos', required: true, type: 'long' },
  ],
}

/**
 * Write a v2 parquet position-delete file. Entries are sorted by
 * (file_path, pos) as required by the Iceberg spec. Returns the
 * DataFile-shaped stats a delete-manifest entry needs.
 *
 * @param {object} options
 * @param {Writer} options.writer
 * @param {{file_path: string, pos: bigint|number}[]} options.deletes
 * @param {CompressionCodec} [options.codec]
 * @returns {Promise<{
 *   record_count: bigint,
 *   value_counts: Record<number, bigint>,
 *   null_value_counts: Record<number, bigint>,
 *   lower_bounds: Record<number, Uint8Array>,
 *   upper_bounds: Record<number, Uint8Array>,
 * }>}
 */
export async function writePositionDeleteFile({ writer, deletes, codec }) {
  if (!deletes?.length) {
    throw new Error('writePositionDeleteFile requires at least one delete')
  }
  const records = deletes.map(d => {
    if (typeof d.file_path !== 'string' || !d.file_path) {
      throw new Error('position delete file_path must be a non-empty string')
    }
    if (d.pos === undefined || d.pos === null) {
      throw new Error('position delete pos is required')
    }
    const pos = typeof d.pos === 'bigint' ? d.pos : BigInt(d.pos)
    if (pos < 0n) throw new Error(`position delete pos must be non-negative: ${pos}`)
    return { file_path: d.file_path, pos }
  })
  records.sort((a, b) => {
    if (a.file_path < b.file_path) return -1
    if (a.file_path > b.file_path) return 1
    if (a.pos < b.pos) return -1
    if (a.pos > b.pos) return 1
    return 0
  })

  await writeParquet({ writer, schema: POSITION_DELETE_SCHEMA, records, codec })

  const { value_counts, null_value_counts, lower_bounds, upper_bounds } =
    computeColumnStats(records, POSITION_DELETE_SCHEMA)

  return {
    record_count: BigInt(records.length),
    value_counts,
    null_value_counts,
    lower_bounds,
    upper_bounds,
  }
}
