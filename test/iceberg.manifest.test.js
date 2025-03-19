import { describe, expect, it } from 'vitest'
import { icebergManifests } from '../src/iceberg.manifest.js'
import { icebergMetadata } from '../src/iceberg.metadata.js'

describe('Iceberg Manifests', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/warehouse/bunnies'

  it('fetches iceberg manifests', async () => {
    const metadata = await icebergMetadata(tableUrl, 'v5.metadata.json')
    const manifests = await icebergManifests(metadata)

    expect(manifests.length).toBe(3)
    const { url, entries } = manifests[2]
    expect(url).toBe('s3a://hyperparam-iceberg/warehouse/bunnies/metadata/c6a8baa0-dac7-41c5-b4db-30aeac1da4e2-m0.avro')
    expect(entries.length).toBe(1)
    const manifest = entries[0]
    expect(manifest).toMatchObject({
      status: 1,
      snapshot_id: 8292582310975252866n,
      sequence_number: null,
      file_sequence_number: null,
      data_file: {
        content: 1,
        file_path: 's3a://hyperparam-iceberg/warehouse/bunnies/data/00000-3-7e317867-0e84-4110-a223-4574f6ec5de5-00001-deletes.parquet',
        file_format: 'PARQUET',
        record_count: 1n,
        file_size_in_bytes: 1457n,
        split_offsets: [4n],
      },
    })
  })
})
