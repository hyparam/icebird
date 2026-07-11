import { describe, expect, it } from 'vitest'
import { resolveAwsCredentials } from '../../src/aws/credentials.js'

describe('resolveAwsCredentials', () => {
  it('returns explicit credentials without importing the AWS SDK', async () => {
    const creds = await resolveAwsCredentials({
      region: 'us-east-1',
      accessKeyId: 'AKID',
      secretAccessKey: 'SECRET',
      sessionToken: 'TOK',
    })
    expect(creds).toEqual({
      accessKeyId: 'AKID',
      secretAccessKey: 'SECRET',
      sessionToken: 'TOK',
      region: 'us-east-1',
    })
  })
})
