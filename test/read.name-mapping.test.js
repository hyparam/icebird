import { parquetWriteBuffer } from 'hyparquet-writer'
import { describe, expect, it } from 'vitest'
import { readDataFile } from '../src/read.js'
import { memResolver } from './helpers.js'

/**
 * @import {ManifestEntry, Schema, TableMetadata} from '../src/types.js'
 */

/**
 * Write a parquet file with no field ids (as a non-iceberg writer would),
 * with a nested struct column, and return a manifest entry pointing at it.
 *
 * @param {ReturnType<typeof memResolver>} mem
 * @returns {ManifestEntry}
 */
function writeIdlessFile(mem) {
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: 'record_id', data: [1, 2] },
      { name: 'label', data: ['a', 'b'] },
      { name: 'loc', data: [{ lat: 1.5, long: -2.5 }, { lat: 3.5, long: -4.5 }] },
    ],
    schema: [
      { name: 'root', num_children: 3 },
      { name: 'record_id', type: 'INT32', repetition_type: 'OPTIONAL' },
      { name: 'label', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'OPTIONAL' },
      { name: 'loc', repetition_type: 'OPTIONAL', num_children: 2 },
      { name: 'lat', type: 'DOUBLE', repetition_type: 'OPTIONAL' },
      { name: 'long', type: 'DOUBLE', repetition_type: 'OPTIONAL' },
    ],
  })
  const path = 'http://test/idless/data.parquet'
  mem.files.set(path, new Uint8Array(buffer))
  return {
    status: 1,
    sequence_number: 1n,
    partition_spec_id: 0,
    data_file: {
      content: 0,
      file_path: path,
      file_format: 'parquet',
      partition: {},
      record_count: 2n,
      file_size_in_bytes: BigInt(buffer.byteLength),
    },
  }
}

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 1,
  fields: [
    { id: 1, name: 'id', required: false, type: 'int' },
    { id: 2, name: 'name', required: false, type: 'string' },
    {
      id: 3, name: 'location', required: false, type: {
        type: 'struct',
        'schema-id': 0,
        fields: [
          { id: 4, name: 'latitude', required: false, type: 'double' },
          { id: 5, name: 'longitude', required: false, type: 'double' },
          { id: 6, name: 'altitude', required: false, type: 'double' },
        ],
      },
    },
    { id: 7, name: 'score', required: false, type: 'double', 'initial-default': 1 },
  ],
}

const nameMapping = [
  { 'field-id': 1, names: ['id', 'record_id'] },
  { 'field-id': 2, names: ['label'] },
  { 'field-id': 3, names: ['loc'], fields: [
    { 'field-id': 4, names: ['lat'] },
    { 'field-id': 5, names: ['long'] },
  ] },
]

/**
 * @param {Record<string, string>} properties
 * @returns {TableMetadata}
 */
function tableMetadata(properties) {
  return {
    'format-version': 2,
    'table-uuid': 'idless-table',
    location: 'http://test/idless',
    'last-sequence-number': 1,
    'last-updated-ms': 0,
    'last-column-id': 7,
    'current-schema-id': 1,
    schemas: [schema],
    'default-spec-id': 0,
    'partition-specs': [{ 'spec-id': 0, fields: [] }],
    'last-partition-id': 999,
    'sort-orders': [{ 'order-id': 0, fields: [] }],
    'default-sort-order-id': 0,
    properties,
  }
}

/**
 * @param {ReturnType<typeof memResolver>} mem
 * @param {ManifestEntry} dataEntry
 * @param {TableMetadata} metadata
 * @returns {Promise<Record<string, any>[]>}
 */
async function readAll(mem, dataEntry, metadata) {
  const rows = []
  for await (const batch of readDataFile({
    dataEntry, fileRowStart: 0, fileRowEnd: 2, schema, metadata, resolver: mem.resolver, rowLineage: false,
  })) rows.push(...batch)
  return rows
}

describe('readDataFile with id-less parquet columns', () => {
  it('maps renamed and nested columns through schema.name-mapping.default', async () => {
    const mem = memResolver()
    const dataEntry = writeIdlessFile(mem)
    const metadata = tableMetadata({ 'schema.name-mapping.default': JSON.stringify(nameMapping) })
    expect(await readAll(mem, dataEntry, metadata)).toEqual([
      { id: 1, name: 'a', location: { latitude: 1.5, longitude: -2.5, altitude: null }, score: 1 },
      { id: 2, name: 'b', location: { latitude: 3.5, longitude: -4.5, altitude: null }, score: 1 },
    ])
  })

  it('falls back to matching by name when the table has no name mapping', async () => {
    const mem = memResolver()
    const dataEntry = writeIdlessFile(mem)
    expect(await readAll(mem, dataEntry, tableMetadata({}))).toEqual([
      { id: null, name: null, location: null, score: 1 },
      { id: null, name: null, location: null, score: 1 },
    ])
  })
})
