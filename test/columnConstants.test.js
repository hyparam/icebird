import { describe, expect, it } from 'vitest'
import { manifestColumnConstant } from '../src/columnConstants.js'
import { serializeValue } from '../src/write/serde.js'
import { icebergCreate } from '../src/create.js'
import { icebergStageAppend } from '../src/write/stage.js'
import { fileCatalogCommit } from '../src/write/commit.js'
import { icebergDataSource } from '../src/sql/icebergDataSource.js'
import { collect, executeSql, readBatchColumn, valueAt } from 'squirreling'
import { memResolver } from './helpers.js'

/** @import {DataFile, Field, Resolver} from '../src/types.js' */

/** @type {Field} */
const field = { id: 1, name: 'name', type: 'string', required: false }
/** @type {DataFile} */
const file = {
  content: 0, file_path: 'unused', file_format: 'parquet', partition: {},
  record_count: 4n, file_size_in_bytes: 1n,
  value_counts: { 1: 4n }, null_value_counts: { 1: 0n },
  lower_bounds: { 1: new TextEncoder().encode('same') },
  upper_bounds: { 1: new TextEncoder().encode('same') },
}

describe('manifest column constants', () => {
  it('requires complete non-null counts and equal enclosing bounds', () => {
    expect(manifestColumnConstant(file, field)).toEqual({ value: 'same' })
    for (const change of [
      { value_counts: undefined }, { null_value_counts: undefined },
      { value_counts: { 1: 3n } }, { null_value_counts: { 1: 1n } },
      { lower_bounds: undefined }, { upper_bounds: undefined },
      { upper_bounds: { 1: new TextEncoder().encode('samf') } },
    ]) expect(manifestColumnConstant({ ...file, ...change }, field)).toBeUndefined()
  })

  it('recognizes all-null columns without relying on bounds', () => {
    expect(manifestColumnConstant({ ...file, null_value_counts: { 1: 4n }, lower_bounds: undefined, upper_bounds: undefined }, field)).toEqual({ value: null })
    expect(manifestColumnConstant({ ...file, record_count: 0n, value_counts: { 1: 0n } }, field)).toBeUndefined()
  })

  it('accepts Avro entry maps and empty strings', () => {
    /** @type {unknown} */
    const avroFile = {
      ...file,
      value_counts: [{ key: 1, value: 4n }], null_value_counts: [{ key: 1, value: 0n }],
      lower_bounds: [{ key: 1, value: new Uint8Array() }], upper_bounds: [{ key: 1, value: new Uint8Array() }],
    }
    expect(manifestColumnConstant(/** @type {DataFile} */ (avroFile), field)).toEqual({ value: '' })
  })

  it('declines floating point and logical conversions and malformed widths', () => {
    for (const type of ['float', 'double', 'timestamp', 'date', 'binary']) {
      expect(manifestColumnConstant(file, { ...field, type: /** @type {Field['type']} */ (type) })).toBeUndefined()
    }
    expect(manifestColumnConstant({ ...file, lower_bounds: { 1: new Uint8Array() }, upper_bounds: { 1: new Uint8Array() } }, { ...field, type: 'boolean' })).toBeUndefined()
  })

  it('preserves the primitive representations of supported types', () => {
    for (const [type, value] of [['boolean', false], ['int', 42], ['long', 42n]]) {
      const t = /** @type {Field['type']} */ (type)
      const bytes = serializeValue(value, t)
      expect(manifestColumnConstant({ ...file, lower_bounds: { 1: bytes }, upper_bounds: { 1: bytes } }, { ...field, type: t })).toEqual({ value })
    }
  })

  it('returns native constants while preserving mixed-null values and filtering', async () => {
    const tableUrl = 'http://test/constants'
    const memory = memResolver()
    let opens = 0
    /** @type {Resolver} */
    const resolver = { ...memory.resolver, reader(path, size) {
      if (path.endsWith('.parquet')) opens++
      return memory.resolver.reader(path, size)
    } }
    const schema = { type: /** @type {const} */ ('struct'), 'schema-id': 0, fields: [
      field,
      { id: 2, name: 'empty', type: /** @type {const} */ ('string'), required: false },
      { id: 3, name: 'mixed', type: /** @type {const} */ ('string'), required: false },
    ] }
    let metadata = await icebergCreate({ tableUrl, schema, resolver })
    const records = Array.from({ length: 4 }, (_, i) => ({ name: 'same', empty: null, mixed: i % 2 ? null : 'same' }))
    const staged = await icebergStageAppend({ tableUrl, metadata, records, resolver })
    metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
    const source = await icebergDataSource({ tableUrl, metadata, resolver })
    opens = 0
    expect(await collect(executeSql({ tables: { t: source }, query: 'SELECT name, COUNT(*) AS n FROM t WHERE name = \'same\' GROUP BY name' }))).toEqual([{ name: 'same', n: 4 }])
    expect(await collect(executeSql({ tables: { t: source }, query: 'SELECT name FROM t WHERE name = \'other\'' }))).toEqual([])
    expect(opens).toBe(0)
    const prepared = source.prepareScan({ columns: schema.fields.map(f => ({ field: f.id, phase: 0, purpose: 'output', mode: 'required' })) })
    for await (const batch of prepared.batches()) {
      expect(batch.columns[0]).toMatchObject({ type: 'constant', value: 'same' })
      expect(batch.columns[1]).toMatchObject({ type: 'constant', value: null })
      const mixed = await readBatchColumn({ batch, columnIndex: 2 })
      expect(Array.from({ length: mixed.length }, (_, i) => valueAt(mixed, i))).toEqual(['same', null, 'same', null])
    }
    expect(await collect(executeSql({ tables: { t: source }, query: 'SELECT * FROM t' }))).toEqual(records)
    expect(await collect(executeSql({ tables: { t: source }, query: 'SELECT name FROM t WHERE mixed IS NULL' }))).toEqual([{ name: 'same' }, { name: 'same' }])
  })
})
