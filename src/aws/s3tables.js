import { loadTable } from '../catalog/loadTable.js'
import { restCatalogConnect } from '../catalog/rest.js'
import { s3SignedResolver } from '../s3.js'
import { createSigV4SignRequest } from '../sigv4.js'
import { resolveAwsCredentials } from './credentials.js'

/**
 * @import {Resolver, RestCatalogContext} from '../types.js'
 * @import {ResolvedAwsCredentials} from './credentials.js'
 */

/** @typedef {RestCatalogContext & { s3TablesCreds?: ResolvedAwsCredentials }} S3TablesCatalogContext */

/**
 * @typedef {object} S3TablesConnectOptions
 * @property {string} region - AWS region, e.g. `us-east-1`
 * @property {string} tableBucketArn - e.g. `arn:aws:s3tables:us-east-1:111122223333:bucket/my-bucket`
 * @property {string} [accessKeyId] - Omit to use the default AWS credential chain
 * @property {string} [secretAccessKey]
 * @property {string} [sessionToken]
 */

/**
 * Iceberg REST endpoint URL for Amazon S3 Tables in a region.
 *
 * @param {string} region
 * @returns {string}
 */
export function s3TablesEndpoint(region) {
  return `https://s3tables.${region}.amazonaws.com/iceberg`
}

/**
 * Connect to the Amazon S3 Tables Iceberg REST catalog for a table bucket.
 *
 * When credentials are resolved from the default chain, the optional peer
 * dependency `@aws-sdk/credential-providers` is required (pass explicit keys to
 * avoid it). Catalog requests are SigV4-signed with service name `s3tables`.
 * Use {@link s3TablesResolver} with the same credentials to read table data
 * files (SigV4 with service name `s3`).
 *
 * @param {S3TablesConnectOptions} options
 * @returns {Promise<S3TablesCatalogContext>}
 */
export async function s3TablesCatalogConnect({
  region, tableBucketArn, accessKeyId, secretAccessKey, sessionToken,
}) {
  const creds = await resolveAwsCredentials({ region, accessKeyId, secretAccessKey, sessionToken })
  const ctx = await restCatalogConnect({
    url: s3TablesEndpoint(region),
    warehouse: tableBucketArn,
    signRequest: createSigV4SignRequest({
      accessKeyId: creds.accessKeyId,
      secretAccessKey: creds.secretAccessKey,
      sessionToken: creds.sessionToken,
      region,
      service: 's3tables',
    }),
  })
  return Object.freeze({ ...ctx, s3TablesCreds: creds })
}

/**
 * Connect using the default AWS credential chain (env vars, shared config, IAM role).
 *
 * @param {object} options
 * @param {string} options.region
 * @param {string} options.tableBucketArn
 * @returns {Promise<S3TablesCatalogContext>}
 */
export function s3TablesCatalogConnectFromEnv({ region, tableBucketArn }) {
  return s3TablesCatalogConnect({ region, tableBucketArn })
}

/**
 * Build a SigV4 `Resolver` for reading S3 Tables data files (`s3://…--table-s3/…`).
 *
 * @param {S3TablesConnectOptions} options
 * @returns {Promise<Resolver>}
 */
export async function s3TablesResolver({ region, accessKeyId, secretAccessKey, sessionToken }) {
  const creds = await resolveAwsCredentials({ region, accessKeyId, secretAccessKey, sessionToken })
  return s3SignedResolver(creds)
}

/**
 * Load a table from an S3 Tables catalog context, wiring a resolver from stored
 * credentials when none is supplied.
 *
 * @param {object} options
 * @param {S3TablesCatalogContext} options.catalog
 * @param {string | string[]} options.namespace
 * @param {string} options.table
 * @param {Resolver} [options.resolver]
 * @returns {ReturnType<typeof loadTable>}
 */
export function loadS3TablesTable({ catalog, namespace, table, resolver }) {
  const eff = resolver ?? (catalog.s3TablesCreds
    ? s3SignedResolver(catalog.s3TablesCreds)
    : undefined)
  return loadTable({ catalog, namespace, table, resolver: eff })
}
