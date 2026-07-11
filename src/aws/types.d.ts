import type { RestCatalogContext } from '../types.js'

export interface ResolvedAwsCredentials {
  accessKeyId: string
  secretAccessKey: string
  sessionToken?: string
  region: string
}

export interface S3TablesCatalogContext extends RestCatalogContext {
  s3TablesCreds?: ResolvedAwsCredentials
}

export interface S3TablesCredentialsOptions {
  /** AWS region, e.g. `us-east-1` */
  region: string
  /** Omit to use the default AWS credential chain */
  accessKeyId?: string
  secretAccessKey?: string
  sessionToken?: string
}

export interface S3TablesConnectOptions extends S3TablesCredentialsOptions {
  /** e.g. `arn:aws:s3tables:us-east-1:111122223333:bucket/my-bucket` */
  tableBucketArn: string
}
