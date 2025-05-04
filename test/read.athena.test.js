import { describe, expect, it } from 'vitest'
import { icebergLatestVersion, icebergListVersions, icebergMetadata, icebergRead } from '../src/index.js'

describe.concurrent('icebergRead from athena table', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/athena/example'

  it('determine latest version', async () => {
    const version = await icebergLatestVersion({ tableUrl })
    expect(version).toEqual('00001-aaf0d033-d06b-43c7-be60-67f83abd4aca')
  })

  it('lists available versions', async () => {
    const versions = await icebergListVersions({ tableUrl })
    expect(versions).toEqual([
      '00000-eedf51e6-8c09-463b-8850-844ee6ec1de0',
      '00001-aaf0d033-d06b-43c7-be60-67f83abd4aca',
    ])
  })

  it('reads aws athena table', async () => {
    const metadataFileName = '00001-aaf0d033-d06b-43c7-be60-67f83abd4aca.metadata.json'
    const metadata = await icebergMetadata({ tableUrl, metadataFileName })
    expect(metadata['last-sequence-number']).toEqual(1)
    expect(metadata['metadata-log']).toEqual([
      {
        'timestamp-ms': 1743474980940,
        'metadata-file': 's3://hyperparam-iceberg/athena/example/metadata/00000-eedf51e6-8c09-463b-8850-844ee6ec1de0.metadata.json',
      },
    ])
    const data = await icebergRead({ tableUrl, metadataFileName, metadata })
    expect(data).toEqual([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ])
  })
})
