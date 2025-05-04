import { icebergRead } from './src/index.js'

const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/spark/bunnies'
const metadataFileName = 'v3.metadata.json'

/**
 * Test script to query data from an Iceberg table.
 */
async function testIcebergQuery() {
  // Read rows 0 through 10 from the bunnies table
  const data = await icebergRead({ tableUrl, rowStart: 0, rowEnd: 10, metadataFileName })
  console.log('Fetched rows:', data.length)
  console.log('First row:', data[0])
}

await testIcebergQuery()
