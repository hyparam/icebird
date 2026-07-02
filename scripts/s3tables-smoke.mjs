import { icebergRead, restCatalogListNamespaces, restCatalogListTables } from '../src/index.js'
import { loadS3TablesTable, s3TablesCatalogConnectFromEnv } from '../src/aws/s3tables.js'

const region = process.env.AWS_REGION ?? 'us-east-1'
const tableBucketArn = process.env.S3TABLES_BUCKET_ARN
const namespace = process.env.S3TABLES_NAMESPACE ?? 'default'
const table = process.env.S3TABLES_TABLE ?? 'orders'

if (!tableBucketArn) {
  console.error('Set S3TABLES_BUCKET_ARN=arn:aws:s3tables:...')
  process.exit(1)
}

console.log(`region=${region} bucket=${tableBucketArn}`)
console.log(`namespace=${namespace} table=${table}\n`)

const catalog = await s3TablesCatalogConnectFromEnv({ region, tableBucketArn })
console.log('connected. prefix:', catalog.prefix)

const namespaces = await restCatalogListNamespaces(catalog)
const tables = await restCatalogListTables(catalog, { namespace })
console.log('namespaces:', JSON.stringify(namespaces))
console.log(`tables in "${namespace}": ${tables.length}`)

const { metadata, tableUrl, resolver } = await loadS3TablesTable({ catalog, namespace, table })
console.log('metadata.location:', metadata.location)
console.log('schema fields:', metadata.schemas?.[metadata['current-schema-id'] ?? 0]?.fields?.map(f => f.name))

const rows = await icebergRead({ tableUrl, metadata, resolver, rowEnd: 10 })

console.log(`\nfirst ${rows.length} row(s) of ${namespace}.${table}:\n`)
const preview = rows.map(row => {
  /** @type {Record<string, unknown>} */
  const out = {}
  for (const [key, value] of Object.entries(row)) {
    if (value instanceof Date) out[key] = value.toISOString()
    else if (typeof value === 'string' && value.length > 80) out[key] = `${value.slice(0, 77)}...`
    else out[key] = value
  }
  return out
})
console.table(preview)
