import { afterEach, describe, expect, it, vi } from 'vitest'
import { createSigV4SignRequest, signRequest } from '../src/sigv4.js'

describe('sigv4', () => {
  afterEach(() => { vi.useRealTimers() })

  it('signRequest uses the configured service in credential scope', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))
    const s3 = await signRequest('GET', 'https://bucket.s3.amazonaws.com/key', undefined, {
      accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: 'us-east-1', service: 's3',
    })
    expect(s3.Authorization).toMatch(/Credential=AKID\/20260101\/us-east-1\/s3\/aws4_request/)

    const s3tables = await signRequest('GET', 'https://s3tables.us-east-1.amazonaws.com/iceberg/v1/config', undefined, {
      accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: 'us-east-1', service: 's3tables',
    })
    expect(s3tables.Authorization).toMatch(/Credential=AKID\/20260101\/us-east-1\/s3tables\/aws4_request/)
  })

  it('signRequest includes session token and content hash headers', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))
    const headers = await signRequest('GET', 'https://example.com/path', undefined, {
      accessKeyId: 'AKID', secretAccessKey: 'SECRET', sessionToken: 'TOK', region: 'us-west-2', service: 'glue',
    })
    expect(headers['x-amz-security-token']).toBe('TOK')
    expect(headers['x-amz-date']).toBe('20260101T000000Z')
    expect(headers['x-amz-content-sha256']).toMatch(/^[0-9a-f]{64}$/)
    expect(headers.Authorization).toMatch(/SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token/)
  })

  it('double-encodes the path for s3tables but not for s3', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))
    // Path carries a URL-encoded warehouse prefix (arn with %3A and %2F). AWS
    // rejects (403) unless s3tables double-URI-encodes it; s3 must not. These
    // golden signatures were verified end-to-end against a live S3 Tables bucket.
    const url = 'https://s3tables.us-east-1.amazonaws.com/iceberg/v1/arn%3Aaws%3As3tables%3Aus-east-1%3A111122223333%3Abucket%2Fmy-bucket/namespaces'
    const creds = { accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: 'us-east-1' }

    const s3tables = await signRequest('GET', url, undefined, { ...creds, service: 's3tables' })
    expect(s3tables.Authorization).toContain(
      'Signature=ca66f79919945a6b3d7a13890a1bebe9d0e87a2378cc3169c95ea5abaa847ffb'
    )

    const s3 = await signRequest('GET', url, undefined, { ...creds, service: 's3' })
    expect(s3.Authorization).toContain(
      'Signature=f2959597b9f9e7540931dbde916c50aa115383dd94211d10e07b1637c0f6ca6b'
    )
  })

  it('createSigV4SignRequest signs fetch init for POST bodies', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))
    const sign = createSigV4SignRequest({
      accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: 'us-east-1', service: 's3tables',
    })
    const init = await sign('https://s3tables.us-east-1.amazonaws.com/iceberg/v1/namespaces/db/tables', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{"requirements":[],"updates":[]}',
    })
    expect(init.method).toBe('POST')
    expect(init.headers?.Authorization).toMatch(/s3tables\/aws4_request/)
    expect(init.headers?.['content-type']).toBe('application/json')
  })
})
