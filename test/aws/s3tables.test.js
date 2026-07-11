import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  loadS3TablesTable,
  s3TablesCatalogConnect,
  s3TablesCatalogConnectFromEnv,
  s3TablesEndpoint,
  s3TablesResolver,
} from '../../src/aws/s3tables.js'
import { restCatalogLoadTable } from '../../src/catalog/rest.js'
import { makeFetch } from '../catalog.rest.helpers.js'

const REGION = 'us-east-1'
const BUCKET_ARN = 'arn:aws:s3tables:us-east-1:111122223333:bucket/my-bucket'
const CONFIG_URL = `${s3TablesEndpoint(REGION)}/v1/config?warehouse=${encodeURIComponent(BUCKET_ARN)}`
const CREDS = { accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: REGION }

const chainCreds = vi.hoisted(() => vi.fn())
vi.mock('@aws-sdk/credential-providers', () => ({
  fromNodeProviderChain: () => chainCreds,
}))

describe('icebird/s3tables', () => {
  /** @type {ReturnType<typeof makeFetch>} */
  let mock

  beforeEach(() => {
    mock = makeFetch({})
    chainCreds.mockReset()
    chainCreds.mockResolvedValue({
      accessKeyId: 'CHAIN',
      secretAccessKey: 'CHAINSECRET',
      sessionToken: 'CHAINTOK',
    })
  })
  afterEach(() => { vi.unstubAllGlobals() })

  it('s3TablesEndpoint returns the regional Iceberg REST URL', () => {
    expect(s3TablesEndpoint('us-west-2')).toBe('https://s3tables.us-west-2.amazonaws.com/iceberg')
  })

  it('s3TablesCatalogConnect fetches config with SigV4 s3tables signing', async () => {
    mock = makeFetch({ [CONFIG_URL]: { defaults: {}, overrides: { prefix: 'encoded-arn' } } })
    vi.stubGlobal('fetch', mock.fn)

    const catalog = await s3TablesCatalogConnect({
      ...CREDS,
      tableBucketArn: BUCKET_ARN,
    })

    expect(chainCreds).not.toHaveBeenCalled()
    expect(mock.calls[0].url).toBe(CONFIG_URL)
    const headers = /** @type {Record<string, string>} */ (mock.calls[0].init?.headers)
    const auth = headers?.authorization ?? headers?.Authorization
    expect(auth).toMatch(/us-east-1\/s3tables\/aws4_request/)
    expect(catalog.type).toBe('rest')
    expect(catalog.url).toBe(s3TablesEndpoint(REGION))
    expect(catalog.prefix).toBe('encoded-arn')
    expect(catalog.s3TablesCreds).toEqual(CREDS)
  })

  it('s3TablesCatalogConnectFromEnv uses the default credential chain', async () => {
    mock = makeFetch({ [CONFIG_URL]: { defaults: {}, overrides: { prefix: 'encoded-arn' } } })
    vi.stubGlobal('fetch', mock.fn)

    const catalog = await s3TablesCatalogConnectFromEnv({
      region: REGION,
      tableBucketArn: BUCKET_ARN,
    })

    expect(chainCreds).toHaveBeenCalled()
    expect(catalog.s3TablesCreds?.accessKeyId).toBe('CHAIN')
  })

  it('s3TablesResolver delegates to s3SignedResolver', async () => {
    const fakeFetch = vi.fn((_url, init) => {
      const headers = /** @type {Record<string, string>} */ (init?.headers)
      expect(headers.Authorization ?? headers.authorization).toMatch(/\/s3\/aws4_request/)
      return Promise.resolve(new Response('', { status: 200, headers: { 'content-length': '0' } }))
    })
    vi.stubGlobal('fetch', fakeFetch)
    const resolver = await s3TablesResolver(CREDS)
    await resolver.reader('s3://uuid--table-s3/data/file.parquet')
  })

  it('loadS3TablesTable wires resolver from catalog credentials', async () => {
    const prefix = 'encoded-arn'
    const loadUrl = `${s3TablesEndpoint(REGION)}/v1/${prefix}/namespaces/analytics/tables/orders`
    mock = makeFetch({
      [CONFIG_URL]: { defaults: {}, overrides: { prefix } },
      [loadUrl]: {
        'metadata-location': 's3://uuid--table-s3/metadata/v1.metadata.json',
        metadata: {
          'format-version': 2,
          'table-uuid': 'u',
          location: 's3://uuid--table-s3/',
          'last-sequence-number': 0,
          'last-updated-ms': 0,
          'last-column-id': 1,
          'current-schema-id': 0,
          schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
          'default-spec-id': 0,
          'partition-specs': [{ 'spec-id': 0, fields: [] }],
          'last-partition-id': 0,
        },
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const catalog = await s3TablesCatalogConnect({ ...CREDS, tableBucketArn: BUCKET_ARN })
    const loaded = await loadS3TablesTable({ catalog, namespace: 'analytics', table: 'orders' })
    expect(loaded.metadata.location).toBe('s3://uuid--table-s3/')
    expect(loaded.tableUrl).toBe('s3://uuid--table-s3/')
    expect(loaded.resolver).toBeDefined()

    const direct = await restCatalogLoadTable(catalog, { namespace: 'analytics', table: 'orders' })
    expect(direct.metadata.location).toBe('s3://uuid--table-s3/')
  })
})
