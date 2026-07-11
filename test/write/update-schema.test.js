import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { icebergCreate } from '../../src/create.js'
import { icebergRead } from '../../src/read.js'
import { icebergStageUpdateSchema } from '../../src/write/stage.js'
import { icebergAppend, icebergUpdateSchema } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Schema} from '../../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: false, type: 'string' },
  ],
}

/** @type {Schema} */
const evolvedSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: false, type: 'string' },
    { id: 3, name: 'score', required: false, type: 'double' },
  ],
}

describe('icebergUpdateSchema', () => {
  it('adds a nullable column and round-trips: old rows read null, new appends use the evolved schema', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/update-schema1'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'alice' }] })

    const updated = await icebergUpdateSchema({ catalog, tableUrl, schema: evolvedSchema })

    expect(updated.schemas).toHaveLength(2)
    expect(updated['current-schema-id']).toBe(1)
    expect(updated.schemas?.[1]['schema-id']).toBe(1)
    expect(updated.schemas?.[1].fields.map(f => f.id)).toEqual([1, 2, 3])
    expect(updated['last-column-id']).toBe(3)
    // metadata-only commit: no new snapshot
    expect(updated.snapshots).toHaveLength(1)

    const committed = await icebergAppend({
      catalog, tableUrl, records: [{ id: 2n, name: 'bob', score: 9.5 }],
    })

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual([
      { id: 1n, name: 'alice', score: null },
      { id: 2n, name: 'bob', score: 9.5 },
    ])
  })

  it('renames a column in place: old data reads under the new name via field id', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/update-schema2'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'alice' }] })

    /** @type {Schema} */
    const renamed = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'full_name', required: false, type: 'string' },
      ],
    }
    const updated = await icebergUpdateSchema({ catalog, tableUrl, schema: renamed })

    expect(updated['last-column-id']).toBe(2)
    const read = await icebergRead({ tableUrl, metadata: updated, resolver })
    expect(read).toEqual([{ id: 1n, full_name: 'alice' }])
  })

  it('rejects reusing an existing field id with an incompatible type', async () => {
    const tableUrl = 'http://test/update-schema3'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    /** @type {Schema} */
    const bad = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'name', required: false, type: 'double' },
      ],
    }
    await expect(icebergUpdateSchema({ catalog, tableUrl, schema: bad }))
      .rejects.toThrow(/cannot promote field name from string to double/)
  })

  it('rejects a new required column without defaults', async () => {
    const tableUrl = 'http://test/update-schema4'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    /** @type {Schema} */
    const bad = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        ...schema.fields,
        { id: 3, name: 'score', required: true, type: 'double' },
      ],
    }
    await expect(icebergUpdateSchema({ catalog, tableUrl, schema: bad }))
      .rejects.toThrow(/required field score \(id 3\) needs a non-null initial-default/)
  })
})

describe('icebergStageUpdateSchema', () => {
  const metadata = /** @type {any} */ ({
    'format-version': 2,
    'table-uuid': 'uuid-1',
    'current-schema-id': 0,
    'last-column-id': 2,
    schemas: [schema],
  })

  it('stages add-schema + set-current-schema with CAS requirements and no snapshot', () => {
    const staged = icebergStageUpdateSchema({ metadata, schema: evolvedSchema })

    expect(staged.snapshot).toBeUndefined()
    expect(staged.writtenFiles).toEqual([])
    expect(staged.requirements).toEqual([
      { type: 'assert-table-uuid', uuid: 'uuid-1' },
      { type: 'assert-current-schema-id', 'current-schema-id': 0 },
      { type: 'assert-last-assigned-field-id', 'last-assigned-field-id': 2 },
    ])
    expect(staged.updates).toEqual([
      { action: 'add-schema', schema: { ...evolvedSchema, 'schema-id': -1 } },
      { action: 'set-current-schema', 'schema-id': -1 },
    ])
  })

  it('throws when schema is not a struct with fields', () => {
    // @ts-expect-error
    expect(() => icebergStageUpdateSchema({ metadata, schema: undefined }))
      .toThrow(/schema must be a struct with a fields array/)
    // @ts-expect-error
    expect(() => icebergStageUpdateSchema({ metadata, schema: { type: 'struct' } }))
      .toThrow(/schema must be a struct with a fields array/)
  })

  it('throws at stage time when a new field id skips past last-column-id validation', () => {
    /** @type {Schema} */
    const bad = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'renamed', required: false, type: 'double' },
      ],
    }
    expect(() => icebergStageUpdateSchema({ metadata, schema: bad }))
      .toThrow(/cannot promote/)
  })
})
