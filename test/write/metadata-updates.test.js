import { afterEach, describe, expect, it, vi } from 'vitest'
import { applyUpdates, fileCatalogCommit } from '../../src/write/commit.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Schema, TableMetadata} from '../../src/types.js'
 */

/** @type {Schema} */
const idSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
}

afterEach(() => {
  vi.restoreAllMocks()
})

/**
 * @param {Partial<TableMetadata>} [overrides]
 * @returns {TableMetadata}
 */
function tableMetadata(overrides = {}) {
  return {
    'format-version': 3,
    'table-uuid': 'u',
    location: 'http://test',
    'last-sequence-number': 0,
    'last-updated-ms': 0,
    'last-column-id': 1,
    'current-schema-id': 0,
    schemas: [idSchema],
    'default-spec-id': 0,
    'partition-specs': [{ 'spec-id': 0, fields: [] }],
    'last-partition-id': 0,
    'sort-orders': [{ 'order-id': 0, fields: [] }],
    'default-sort-order-id': 0,
    'next-row-id': 0,
    ...overrides,
  }
}

describe('metadata schema updates', () => {
  it('applies add-schema and set-current-schema updates', () => {
    /** @type {Schema} */
    const nextSchema = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        ...idSchema.fields,
        {
          id: 99,
          name: 'tag',
          required: false,
          type: 'string',
          'initial-default': null,
          'write-default': 'unknown',
        },
      ],
    }

    const next = applyUpdates(tableMetadata(), [
      { action: 'add-schema', schema: nextSchema },
      { action: 'set-current-schema', 'schema-id': -1 },
    ])

    expect(next.schemas).toHaveLength(2)
    expect(next.schemas[1]['schema-id']).toBe(1)
    expect(next.schemas[1].fields.find(f => f.name === 'tag')?.['write-default']).toBe('unknown')
    expect(next['current-schema-id']).toBe(1)
    expect(next['last-column-id']).toBe(99)
  })

  it('updates last-column-id from nested field ids in an added schema', () => {
    /** @type {Schema} */
    const nextSchema = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        ...idSchema.fields,
        {
          id: 2,
          name: 'tags',
          required: false,
          type: { type: 'list', 'element-id': 3, 'element-required': false, element: 'string' },
        },
      ],
    }

    const next = applyUpdates(tableMetadata(), [
      { action: 'add-schema', schema: nextSchema },
      { action: 'set-current-schema', 'schema-id': -1 },
    ])

    expect(next['current-schema-id']).toBe(1)
    expect(next['last-column-id']).toBe(3)
  })

  it('rejects add-schema with write-default on a v2 table', () => {
    /** @type {Schema} */
    const schemaWithDefault = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        { id: 2, name: 'tag', required: false, type: 'string', 'write-default': 'unknown' },
      ],
    }

    expect(() => applyUpdates(tableMetadata({
      'format-version': 2,
      schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
      'next-row-id': undefined,
    }), [
      { action: 'add-schema', schema: schemaWithDefault },
    ])).toThrow(/write-default requires format-version 3/)
  })

  it('rejects add-schema when a new required field lacks required defaults', () => {
    /** @type {Schema} */
    const newRequiredNoDefault = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        ...idSchema.fields,
        { id: 2, name: 'tag', required: true, type: 'string' },
      ],
    }
    expect(() => applyUpdates(tableMetadata(), [
      { action: 'add-schema', schema: newRequiredNoDefault },
    ])).toThrow(/required field tag .* needs a non-null initial-default/)

    expect(() => applyUpdates(tableMetadata(), [
      {
        action: 'add-schema',
        schema: {
          type: 'struct',
          'schema-id': -1,
          fields: [
            ...idSchema.fields,
            { id: 2, name: 'tag', required: true, type: 'string', 'initial-default': 'unknown' },
          ],
        },
      },
    ])).toThrow(/required field tag .* needs a non-null write-default/)

    /** @type {Schema} */
    const newRequiredWithDefaults = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        ...idSchema.fields,
        {
          id: 2,
          name: 'tag',
          required: true,
          type: 'string',
          'initial-default': 'unknown',
          'write-default': 'unknown',
        },
      ],
    }
    expect(() => applyUpdates(tableMetadata(), [
      { action: 'add-schema', schema: newRequiredWithDefaults },
    ])).not.toThrow()
  })

  it('rejects add-schema when an existing field changes initial-default', () => {
    /** @type {Schema} */
    const withDefault = {
      type: 'struct',
      'schema-id': 1,
      fields: [
        ...idSchema.fields,
        {
          id: 2,
          name: 'tag',
          required: false,
          type: 'string',
          'initial-default': 'old',
          'write-default': 'new',
        },
      ],
    }
    /** @type {Schema} */
    const changedDefault = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        ...idSchema.fields,
        {
          id: 2,
          name: 'tag',
          required: false,
          type: 'string',
          'initial-default': 'changed',
          'write-default': 'new',
        },
      ],
    }

    expect(() => applyUpdates(tableMetadata({
      'last-column-id': 2,
      'current-schema-id': 1,
      schemas: [idSchema, withDefault],
    }), [
      { action: 'add-schema', schema: changedDefault },
    ])).toThrow(/initial-default.*tag.*cannot change/)
  })

  it('rejects add-schema with an invalid primitive type change', () => {
    /** @type {Schema} */
    const invalidTypeChange = {
      type: 'struct',
      'schema-id': -1,
      fields: [{ id: 1, name: 'id', required: true, type: 'string' }],
    }

    expect(() => applyUpdates(tableMetadata(), [
      { action: 'add-schema', schema: invalidTypeChange },
    ])).toThrow(/cannot promote field id from long to string/)
  })

  it('rejects add-schema with a duplicate schema-id', () => {
    expect(() => applyUpdates(tableMetadata(), [
      { action: 'add-schema', schema: { type: 'struct', 'schema-id': 0, fields: [] } },
    ])).toThrow(/schema-id 0 already exists/)
  })

  it('rejects set-current-schema with an unknown schema-id', () => {
    expect(() => applyUpdates(tableMetadata(), [
      { action: 'set-current-schema', 'schema-id': 7 },
    ])).toThrow(/schema-id 7 not found/)
  })

  it('updates last-updated-ms for metadata-only schema commits', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000001234)
    const { resolver } = memResolver()
    const metadata = tableMetadata({ 'last-updated-ms': 1700000000000 })
    /** @type {Schema} */
    const nextSchema = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        ...idSchema.fields,
        { id: 2, name: 'tag', required: false, type: 'string', 'initial-default': null, 'write-default': null },
      ],
    }

    const committed = await fileCatalogCommit({
      tableUrl: 'http://test/metadata-only-schema',
      metadata,
      resolver,
      staged: {
        snapshot: /** @type {any} */ (null),
        requirements: [{ type: 'assert-table-uuid', uuid: metadata['table-uuid'] }],
        updates: [
          { action: 'add-schema', schema: nextSchema },
          { action: 'set-current-schema', 'schema-id': -1 },
        ],
        writtenFiles: [],
      },
    })

    expect(committed['last-updated-ms']).toBe(1700000001234)
  })
})

describe('metadata partition spec updates', () => {
  it('applies add-spec and set-default-spec updates', () => {
    const next = applyUpdates(tableMetadata({ 'last-partition-id': 999 }), [
      {
        action: 'add-spec',
        spec: {
          'spec-id': -1,
          fields: [{ 'source-id': 1, 'field-id': 1001, name: 'id_bucket', transform: 'bucket[8]' }],
        },
      },
      { action: 'set-default-spec', 'spec-id': -1 },
    ])

    expect(next['partition-specs']).toHaveLength(2)
    expect(next['partition-specs'][1]['spec-id']).toBe(1)
    expect(next['partition-specs'][1].fields[0]['source-id']).toBe(1)
    expect(next['last-partition-id']).toBe(1001)
    expect(next['default-spec-id']).toBe(1)
  })

  it('rejects add-spec with a duplicate spec-id', () => {
    expect(() => applyUpdates(tableMetadata(), [
      { action: 'add-spec', spec: { 'spec-id': 0, fields: [] } },
    ])).toThrow(/spec-id 0 already exists/)
  })

  it('rejects add-spec when an equivalent partition spec already exists', () => {
    const identityCategorySpec = {
      'spec-id': 0,
      fields: [{ 'source-id': 2, 'field-id': 1000, name: 'category', transform: 'identity' }],
    }
    const categorySchema = {
      type: /** @type {const} */ ('struct'),
      'schema-id': 0,
      fields: [
        ...idSchema.fields,
        { id: 2, name: 'category', required: true, type: /** @type {const} */ ('string') },
      ],
    }

    expect(() => applyUpdates(tableMetadata({
      'last-column-id': 2,
      schemas: [categorySchema],
      'partition-specs': [identityCategorySpec],
      'last-partition-id': 1000,
    }), [
      {
        action: 'add-spec',
        spec: {
          'spec-id': -1,
          fields: [{ 'source-id': 2, 'field-id': 1000, name: 'category', transform: 'identity' }],
        },
      },
    ])).toThrow(/equivalent partition spec already exists/)
  })

  it('rejects set-default-spec with an unknown spec-id', () => {
    expect(() => applyUpdates(tableMetadata(), [
      { action: 'set-default-spec', 'spec-id': 7 },
    ])).toThrow(/spec-id 7 not found/)
  })
})
