import { readIcebergData } from './src/iceberg.js'

/**
 * Test script to query data from an Iceberg table
 * @param {string} tableUrl - The base URL of the Iceberg table
 * @param {number} startRow - The starting row to fetch (inclusive)
 * @param {number} endRow - The ending row to fetch (inclusive)
 * @returns {Promise<void>}
 */
async function testIcebergQuery(tableUrl, startRow, endRow) {
  const data = await readIcebergData(tableUrl, startRow, endRow)
  console.log('Fetched rows:', data.length)
  console.log('First row:', data[0])
}

// Read rows 0 through 99 from the bunnies table
const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/warehouse/bunnies'
await testIcebergQuery({ tableUrl, rowStart: 0, rowEnd: 99, metadataFileName: 'v2.metadata.json' })
