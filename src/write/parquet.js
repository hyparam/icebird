import { parquetWrite } from 'hyparquet-writer'
import { sanitize } from '../utils.js'

/**
 * @import {Writer} from 'hyparquet-writer/src/types.js'
 * @import {BasicType, ColumnSource} from 'hyparquet-writer/src/types.js'
 * @import {DecodedArray} from 'hyparquet'
 * @import {Field, Schema} from '../../src/types.js'
 */

/**
 * Write iceberg records to parquet, embedding the iceberg schema in the
 * parquet key/value metadata so that the read path can map field ids back to
 * physical column names.
 *
 * @param {object} options
 * @param {Writer} options.writer
 * @param {Schema} options.schema
 * @param {Record<string, any>[]} options.records
 */
export function writeParquet({ writer, schema, records }) {
  const columnData = schema.fields.map(field => ({
    name: sanitize(field.name),
    data: extractColumn(records, field),
    type: icebergTypeToParquet(field.type),
    nullable: !field.required,
  }))

  parquetWrite({
    writer,
    columnData,
    kvMetadata: [{ key: 'iceberg.schema', value: JSON.stringify(schema) }],
  })
}

/**
 * @param {Record<string, any>[]} records
 * @param {Field} field
 * @returns {DecodedArray}
 */
function extractColumn(records, field) {
  const out = new Array(records.length)
  for (let i = 0; i < records.length; i++) {
    const v = records[i][field.name]
    out[i] = v === undefined ? null : v
  }
  return out
}

/**
 * @param {string} type
 * @returns {BasicType}
 */
function icebergTypeToParquet(type) {
  switch (type) {
  case 'boolean': return 'BOOLEAN'
  case 'int': return 'INT32'
  case 'long': return 'INT64'
  case 'float': return 'FLOAT'
  case 'double': return 'DOUBLE'
  case 'string': return 'STRING'
  case 'binary': return 'BYTE_ARRAY'
  case 'uuid': return 'UUID'
  case 'timestamp':
  case 'timestamptz': return 'TIMESTAMP'
  default: throw new Error(`unsupported iceberg type: ${type}`)
  }
}
