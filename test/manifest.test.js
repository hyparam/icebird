import { describe, expect, it } from 'vitest'
import fs from 'fs'
import { icebergManifests } from '../src/manifest.js'
import { icebergMetadata } from '../src/metadata.js'
import { localResolver } from './helpers.js'

describe('Iceberg Manifests', () => {
  const tableUrl = 's3://hyperparam-iceberg/spark/bunnies'
  const resolver = localResolver('test/files')

  it('fetches iceberg manifests', async () => {
    const metadataFileName = 'v5.metadata.json'
    const metadata = await icebergMetadata({ tableUrl, metadataFileName, resolver })
    const manifests = await icebergManifests({ metadata, resolver })

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

  it('passes manifest length to the resolver when fetching manifest files', async () => {
    const manifestPath = 's3a://hyperparam-iceberg/spark/bunnies/metadata/ac75cc9f-cc0c-4712-8337-fe4b0c473459-m0.avro'
    const manifestLength = fs.statSync(
      'test/files/hyperparam-iceberg/spark/bunnies/metadata/ac75cc9f-cc0c-4712-8337-fe4b0c473459-m0.avro'
    ).size
    /** @type {{url: string, byteLength?: number}[]} */
    const calls = []
    /** @type {import('../src/types.js').Resolver} */
    const countingResolver = {
      reader(url, byteLength) {
        calls.push({ url, byteLength })
        return resolver.reader(url, byteLength)
      },
    }
    /** @type {import('../src/types.js').TableMetadata} */
    const metadata = {
      'format-version': 2,
      'table-uuid': 'test',
      location: tableUrl,
      'last-sequence-number': 0,
      'last-updated-ms': 0,
      'last-column-id': 0,
      'current-schema-id': 0,
      schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
      'current-snapshot-id': 1,
      snapshots: [{
        'snapshot-id': 1,
        'sequence-number': 1,
        'timestamp-ms': 0,
        'manifest-list': '',
        summary: { operation: 'append' },
        manifests: [{
          manifest_path: manifestPath,
          manifest_length: BigInt(manifestLength),
          partition_spec_id: 0,
          content: 1,
          added_snapshot_id: 1n,
          added_files_count: 1,
          existing_files_count: 0,
          deleted_files_count: 0,
          added_rows_count: 1n,
          existing_rows_count: 0n,
          deleted_rows_count: 0n,
        }],
      }],
    }

    const manifests = await icebergManifests({ metadata, resolver: countingResolver })

    expect(manifests).toHaveLength(1)
    expect(calls).toEqual([{ url: manifestPath, byteLength: manifestLength }])
  })
})
