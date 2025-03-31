import { describe, expect, it } from 'vitest'
import { icebergLatestVersion, icebergRead } from '../src/iceberg.js'

describe.concurrent('icebergRead from athena table', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/athena/example'

  it('determine latest version', async () => {
    const version = await icebergLatestVersion({ tableUrl })
    expect(version).toEqual('00001-aaf0d033-d06b-43c7-be60-67f83abd4aca')
  })

  it('reads aws athena table', async () => {
    const metadataFileName = '00001-aaf0d033-d06b-43c7-be60-67f83abd4aca.metadata.json'
    const data = await icebergRead({ tableUrl, metadataFileName })
    expect(data).toEqual([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ])
  })
})
