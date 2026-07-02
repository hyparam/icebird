/**
 * @typedef {object} ResolvedAwsCredentials
 * @property {string} accessKeyId
 * @property {string} secretAccessKey
 * @property {string} [sessionToken]
 * @property {string} region
 */

/**
 * Resolve AWS credentials from explicit keys or the default Node provider chain.
 *
 * The optional peer dependency `@aws-sdk/credential-providers` is imported
 * lazily and only when falling back to the chain, so passing explicit keys
 * keeps this module free of the AWS SDK (and browser-compatible).
 *
 * @param {object} options
 * @param {string} options.region
 * @param {string} [options.accessKeyId]
 * @param {string} [options.secretAccessKey]
 * @param {string} [options.sessionToken]
 * @returns {Promise<ResolvedAwsCredentials>}
 */
export async function resolveAwsCredentials({
  region, accessKeyId, secretAccessKey, sessionToken,
}) {
  if (accessKeyId && secretAccessKey) {
    return { accessKeyId, secretAccessKey, sessionToken, region }
  }
  let fromNodeProviderChain
  try {
    ;({ fromNodeProviderChain } = await import('@aws-sdk/credential-providers'))
  } catch (err) {
    const { code } = /** @type {NodeJS.ErrnoException} */ (err)
    if (code === 'ERR_MODULE_NOT_FOUND') {
      throw new Error(
        'Cannot find module \'@aws-sdk/credential-providers\'. '
        + 'Install the optional peer dependency: npm install @aws-sdk/credential-providers'
      )
    }
    throw err
  }
  const provider = fromNodeProviderChain({ clientConfig: { region } })
  const creds = await provider()
  return {
    accessKeyId: creds.accessKeyId,
    secretAccessKey: creds.secretAccessKey,
    sessionToken: creds.sessionToken,
    region,
  }
}
