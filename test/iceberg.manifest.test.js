import { describe, expect, it } from 'vitest'
import { icebergManifests } from '../src/iceberg.manifest.js'
import { icebergMetadata } from '../src/iceberg.metadata.js'

describe('Iceberg Manifests', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/spark/bunnies'

  it('fetches iceberg manifests', async () => {
    const metadataFileName = 'v5.metadata.json'
    const metadata = await icebergMetadata({ tableUrl, metadataFileName })
    const manifests = await icebergManifests(metadata)

    expect(manifests.length).toBe(3)
    const { url, entries } = manifests[2]
    expect(url).toBe('s3a://hyperparam-iceberg/spark/bunnies/metadata/ac75cc9f-cc0c-4712-8337-fe4b0c473459-m0.avro')
    expect(entries.length).toBe(1)
    const manifest = entries[0]
    expect(manifest).toMatchObject({
      status: 1,
      snapshot_id: 469881615898633426n,
      sequence_number: 2n,
      file_sequence_number: 2n,
      data_file: {
        content: 1,
        file_path: 's3a://hyperparam-iceberg/spark/bunnies/data/00000-3-6fdcdaeb-591f-4ae0-a39a-75c7fba53907-00001-deletes.parquet',
        file_format: 'PARQUET',
        record_count: 1n,
        file_size_in_bytes: 1437n,
        split_offsets: [4n],
      },
    })
  })
})
