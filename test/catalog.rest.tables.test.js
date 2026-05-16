import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  restCatalogConnect,
  restCatalogCreateTable,
  restCatalogDropTable,
  restCatalogListTables,
  restCatalogLoadCredentials,
  restCatalogLoadTable,
  restCatalogRegisterTable,
  restCatalogRenameTable,
  restCatalogUpdateTable,
} from '../src/catalog/rest.js'
import { makeFetch } from './catalog.rest.helpers.js'

describe('REST Catalog client — tables', () => {
  /** @type {ReturnType<typeof makeFetch>} */
  let mock

  beforeEach(() => { mock = makeFetch({}) })
  afterEach(() => { vi.unstubAllGlobals() })

  it('restCatalogListTables paginates and applies prefix', async () => {
    mock = makeFetch({
      'https://cat/v1/config': { overrides: { prefix: 'ws/main' } },
      'https://cat/v1/ws/main/namespaces/db/tables': {
        identifiers: [{ namespace: ['db'], name: 't1' }],
        'next-page-token': 'p2',
      },
      'https://cat/v1/ws/main/namespaces/db/tables?pageToken=p2': {
        identifiers: [{ namespace: ['db'], name: 't2' }],
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const tables = await restCatalogListTables(ctx, { namespace: 'db' })
    expect(tables).toEqual([
      { namespace: ['db'], name: 't1' },
      { namespace: ['db'], name: 't2' },
    ])
  })

  it('restCatalogLoadTable returns metadata and metadataLocation', async () => {
    const metadata = {
      'format-version': 2,
      'table-uuid': 'abc',
      location: 's3://bucket/table',
      schemas: [],
    }
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/tables/orders': {
        'metadata-location': 's3://bucket/table/metadata/v3.metadata.json',
        metadata,
        config: { 'client.region': 'us-east-1' },
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const result = await restCatalogLoadTable(ctx, { namespace: 'db', table: 'orders' })
    expect(result.metadataLocation).toBe('s3://bucket/table/metadata/v3.metadata.json')
    expect(result.metadata).toEqual(metadata)
    expect(result.config).toEqual({ 'client.region': 'us-east-1' })
  })

  it('restCatalogLoadTable encodes multi-level namespace with %1F', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db%1Fsub/tables/orders': {
        'metadata-location': 'x',
        metadata: { location: 's3://x' },
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogLoadTable(ctx, { namespace: ['db', 'sub'], table: 'orders' })
    expect(mock.calls.some(c => c.url === 'https://cat/v1/namespaces/db%1Fsub/tables/orders')).toBe(true)
  })

  it('restCatalogDropTable issues DELETE and applies prefix', async () => {
    mock = makeFetch({
      'https://cat/v1/config': { overrides: { prefix: 'ws/main' } },
      'https://cat/v1/ws/main/namespaces/db/tables/orders': () => new Response(null, { status: 204 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogDropTable(ctx, { namespace: 'db', table: 'orders' })

    const dropCall = mock.calls.find(c => c.url === 'https://cat/v1/ws/main/namespaces/db/tables/orders')
    expect(dropCall).toBeDefined()
    expect(dropCall?.init?.method).toBe('DELETE')
  })

  it('restCatalogDropTable forwards purgeRequested as query param', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/tables/orders?purgeRequested=true': () => new Response(null, { status: 204 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogDropTable(ctx, { namespace: 'db', table: 'orders', purgeRequested: true })

    expect(mock.calls.some(c => c.url === 'https://cat/v1/namespaces/db/tables/orders?purgeRequested=true')).toBe(true)
  })

  it('restCatalogDropTable encodes multi-level namespace with %1F', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db%1Fsub/tables/orders': () => new Response(null, { status: 204 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogDropTable(ctx, { namespace: ['db', 'sub'], table: 'orders' })

    expect(mock.calls.some(c => c.url === 'https://cat/v1/namespaces/db%1Fsub/tables/orders')).toBe(true)
  })

  it('restCatalogDropTable surfaces ErrorModel on failure', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/tables/missing': () => new Response(JSON.stringify({
        error: { code: 404, type: 'NoSuchTableException', message: 'Table not found' },
      }), { status: 404 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await expect(restCatalogDropTable(ctx, { namespace: 'db', table: 'missing' }))
      .rejects.toThrow('404 NoSuchTableException: Table not found')
  })

  it('restCatalogCreateTable POSTs name and schema, applies prefix', async () => {
    const schema = {
      type: /** @type {const} */ ('struct'),
      'schema-id': 0,
      fields: [{ id: 1, name: 'id', required: true, type: /** @type {const} */ ('long') }],
    }
    const metadata = { 'format-version': 2, 'table-uuid': 'abc', location: 's3://bucket/orders', schemas: [schema] }
    mock = makeFetch({
      'https://cat/v1/config': { overrides: { prefix: 'ws/main' } },
      'https://cat/v1/ws/main/namespaces/db/tables': {
        'metadata-location': 's3://bucket/orders/metadata/v1.metadata.json',
        metadata,
        config: { 'client.region': 'us-east-1' },
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const result = await restCatalogCreateTable(ctx, {
      namespace: 'db',
      table: 'orders',
      schema,
    })

    expect(result.metadataLocation).toBe('s3://bucket/orders/metadata/v1.metadata.json')
    expect(result.metadata).toEqual(metadata)
    expect(result.config).toEqual({ 'client.region': 'us-east-1' })

    const call = mock.calls.find(c => c.url === 'https://cat/v1/ws/main/namespaces/db/tables')
    expect(call?.init?.method).toBe('POST')
    const headers = /** @type {Record<string,string>} */ (call?.init?.headers)
    expect(headers['content-type']).toBe('application/json')
    expect(JSON.parse(/** @type {string} */ (call?.init?.body))).toEqual({
      name: 'orders',
      schema,
    })
  })

  it('restCatalogCreateTable forwards optional fields and encodes multi-level namespace', async () => {
    const schema = { type: 'struct', 'schema-id': 0, fields: [] }
    const partitionSpec = { 'spec-id': 0, fields: [{ 'source-id': 1, 'field-id': 1000, name: 'id', transform: 'identity' }] }
    const writeOrder = { 'order-id': 1, fields: [{ transform: 'identity', 'source-id': 1, direction: 'asc', 'null-order': 'nulls-first' }] }
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db%1Fsub/tables': {
        'metadata-location': 's3://b/m.json',
        metadata: { location: 's3://b' },
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogCreateTable(ctx, {
      namespace: ['db', 'sub'],
      table: 'orders',
      schema: /** @type {any} */ (schema),
      location: 's3://b/orders',
      partitionSpec: /** @type {any} */ (partitionSpec),
      writeOrder: /** @type {any} */ (writeOrder),
      stageCreate: true,
      properties: { 'write.format.default': 'parquet' },
    })

    const call = mock.calls.find(c => c.url === 'https://cat/v1/namespaces/db%1Fsub/tables')
    expect(call).toBeDefined()
    expect(JSON.parse(/** @type {string} */ (call?.init?.body))).toEqual({
      name: 'orders',
      schema,
      location: 's3://b/orders',
      'partition-spec': partitionSpec,
      'write-order': writeOrder,
      'stage-create': true,
      properties: { 'write.format.default': 'parquet' },
    })
  })

  it('restCatalogCreateTable surfaces ErrorModel on conflict', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/tables': () => new Response(JSON.stringify({
        error: { code: 409, type: 'AlreadyExistsException', message: 'table exists' },
      }), { status: 409 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await expect(restCatalogCreateTable(ctx, {
      namespace: 'db',
      table: 'orders',
      schema: /** @type {any} */ ({ type: 'struct', 'schema-id': 0, fields: [] }),
    })).rejects.toThrow('409 AlreadyExistsException: table exists')
  })

  it('restCatalogRegisterTable POSTs name and metadata-location', async () => {
    const metadata = { 'format-version': 2, 'table-uuid': 'abc', location: 's3://bucket/orders', schemas: [] }
    mock = makeFetch({
      'https://cat/v1/config': { overrides: { prefix: 'ws/main' } },
      'https://cat/v1/ws/main/namespaces/db/register': {
        'metadata-location': 's3://bucket/orders/metadata/v1.metadata.json',
        metadata,
        config: { 'client.region': 'us-east-1' },
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const result = await restCatalogRegisterTable(ctx, {
      namespace: 'db',
      table: 'orders',
      metadataLocation: 's3://bucket/orders/metadata/v1.metadata.json',
    })

    expect(result.metadataLocation).toBe('s3://bucket/orders/metadata/v1.metadata.json')
    expect(result.metadata).toEqual(metadata)
    expect(result.config).toEqual({ 'client.region': 'us-east-1' })

    const call = mock.calls.find(c => c.url === 'https://cat/v1/ws/main/namespaces/db/register')
    expect(call?.init?.method).toBe('POST')
    const headers = /** @type {Record<string,string>} */ (call?.init?.headers)
    expect(headers['content-type']).toBe('application/json')
    expect(JSON.parse(/** @type {string} */ (call?.init?.body))).toEqual({
      name: 'orders',
      'metadata-location': 's3://bucket/orders/metadata/v1.metadata.json',
    })
  })

  it('restCatalogRegisterTable forwards overwrite and encodes multi-level namespace', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db%1Fsub/register': {
        'metadata-location': 's3://b/m.json',
        metadata: { location: 's3://b' },
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogRegisterTable(ctx, {
      namespace: ['db', 'sub'],
      table: 'orders',
      metadataLocation: 's3://b/m.json',
      overwrite: true,
    })

    const call = mock.calls.find(c => c.url === 'https://cat/v1/namespaces/db%1Fsub/register')
    expect(call).toBeDefined()
    expect(JSON.parse(/** @type {string} */ (call?.init?.body))).toEqual({
      name: 'orders',
      'metadata-location': 's3://b/m.json',
      overwrite: true,
    })
  })

  it('restCatalogRegisterTable surfaces ErrorModel on conflict', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/register': () => new Response(JSON.stringify({
        error: { code: 409, type: 'AlreadyExistsException', message: 'table exists' },
      }), { status: 409 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await expect(restCatalogRegisterTable(ctx, {
      namespace: 'db',
      table: 'orders',
      metadataLocation: 's3://b/m.json',
    })).rejects.toThrow('409 AlreadyExistsException: table exists')
  })

  it('restCatalogRenameTable POSTs source and destination', async () => {
    mock = makeFetch({
      'https://cat/v1/config': { overrides: { prefix: 'ws/main' } },
      'https://cat/v1/ws/main/tables/rename': () => new Response(null, { status: 204 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogRenameTable(ctx, {
      source: { namespace: ['db'], name: 'orders' },
      destination: { namespace: ['db'], name: 'orders_v2' },
    })

    const call = mock.calls.find(c => c.url === 'https://cat/v1/ws/main/tables/rename')
    expect(call?.init?.method).toBe('POST')
    expect(JSON.parse(/** @type {string} */ (call?.init?.body))).toEqual({
      source: { namespace: ['db'], name: 'orders' },
      destination: { namespace: ['db'], name: 'orders_v2' },
    })
  })

  it('restCatalogRenameTable surfaces ErrorModel on failure', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/tables/rename': () => new Response(JSON.stringify({
        error: { code: 409, type: 'AlreadyExistsException', message: 'destination exists' },
      }), { status: 409 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await expect(restCatalogRenameTable(ctx, {
      source: { namespace: ['db'], name: 'a' },
      destination: { namespace: ['db'], name: 'b' },
    })).rejects.toThrow('409 AlreadyExistsException: destination exists')
  })

  it('restCatalogUpdateTable POSTs requirements and updates, applies prefix', async () => {
    const metadata = { 'format-version': 2, 'table-uuid': 'abc', location: 's3://bucket/orders', schemas: [] }
    mock = makeFetch({
      'https://cat/v1/config': { overrides: { prefix: 'ws/main' } },
      'https://cat/v1/ws/main/namespaces/db/tables/orders': {
        'metadata-location': 's3://bucket/orders/metadata/v4.metadata.json',
        metadata,
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const requirements = /** @type {any} */ ([
      { type: 'assert-table-uuid', uuid: 'abc' },
      { type: 'assert-ref-snapshot-id', ref: 'main', 'snapshot-id': 42 },
    ])
    const updates = /** @type {any} */ ([
      { action: 'set-snapshot-ref', 'ref-name': 'main', type: 'branch', 'snapshot-id': 99 },
    ])

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const result = await restCatalogUpdateTable(ctx, {
      namespace: 'db',
      table: 'orders',
      requirements,
      updates,
    })

    expect(result.metadataLocation).toBe('s3://bucket/orders/metadata/v4.metadata.json')
    expect(result.metadata).toEqual(metadata)
    expect(result.config).toEqual({})

    const call = mock.calls.find(c => c.url === 'https://cat/v1/ws/main/namespaces/db/tables/orders')
    expect(call?.init?.method).toBe('POST')
    const headers = /** @type {Record<string,string>} */ (call?.init?.headers)
    expect(headers['content-type']).toBe('application/json')
    expect(JSON.parse(/** @type {string} */ (call?.init?.body))).toEqual({ requirements, updates })
  })

  it('restCatalogUpdateTable encodes multi-level namespace with %1F', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db%1Fsub/tables/orders': {
        'metadata-location': 's3://b/m.json',
        metadata: { location: 's3://b' },
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogUpdateTable(ctx, {
      namespace: ['db', 'sub'],
      table: 'orders',
      requirements: [],
      updates: [],
    })

    expect(mock.calls.some(c => c.url === 'https://cat/v1/namespaces/db%1Fsub/tables/orders')).toBe(true)
  })

  it('restCatalogUpdateTable surfaces CommitFailedException', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/tables/orders': () => new Response(JSON.stringify({
        error: { code: 409, type: 'CommitFailedException', message: 'requirement failed' },
      }), { status: 409 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await expect(restCatalogUpdateTable(ctx, {
      namespace: 'db',
      table: 'orders',
      requirements: /** @type {any} */ ([{ type: 'assert-table-uuid', uuid: 'abc' }]),
      updates: [],
    })).rejects.toThrow('409 CommitFailedException: requirement failed')
  })

  it('restCatalogLoadCredentials returns storage-credentials array', async () => {
    mock = makeFetch({
      'https://cat/v1/config': { overrides: { prefix: 'ws/main' } },
      'https://cat/v1/ws/main/namespaces/db/tables/orders/credentials': {
        'storage-credentials': [
          { prefix: 's3://bucket/orders/', config: { 's3.access-key-id': 'AK', 's3.secret-access-key': 'SK' } },
          { prefix: 's3://bucket/orders/staging/', config: { 's3.session-token': 'tok' } },
        ],
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const creds = await restCatalogLoadCredentials(ctx, { namespace: 'db', table: 'orders' })

    expect(creds).toEqual([
      { prefix: 's3://bucket/orders/', config: { 's3.access-key-id': 'AK', 's3.secret-access-key': 'SK' } },
      { prefix: 's3://bucket/orders/staging/', config: { 's3.session-token': 'tok' } },
    ])
  })

  it('restCatalogLoadCredentials defaults to empty array when omitted', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db%1Fsub/tables/orders/credentials': {},
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const creds = await restCatalogLoadCredentials(ctx, { namespace: ['db', 'sub'], table: 'orders' })
    expect(creds).toEqual([])
  })

  it('restCatalogLoadCredentials surfaces ErrorModel on failure', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/tables/missing/credentials': () => new Response(JSON.stringify({
        error: { code: 404, type: 'NoSuchTableException', message: 'Table not found' },
      }), { status: 404 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await expect(restCatalogLoadCredentials(ctx, { namespace: 'db', table: 'missing' }))
      .rejects.toThrow('404 NoSuchTableException: Table not found')
  })
})
