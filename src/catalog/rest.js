/**
 * Iceberg REST Catalog client.
 *
 * Plain async functions over a stateless context object — no classes.
 * The catalog client never imports from the read path; callers glue the
 * two together by passing `metadata` and `metadata.location` from
 * `restCatalogLoadTable` into `icebergRead`.
 *
 * @import {LoadTableResponse, PartitionSpec, RestCatalogContext, Schema, SortOrder, StorageCredential, TableIdentifier, TableMetadata, TableRequirement, TableUpdate} from '../../src/types.js'
 */

/**
 * Connect to a REST catalog by fetching `/v1/config`.
 * Returns a frozen context object that holds the prefix, defaults, overrides
 * and the user-supplied requestInit (for auth) for use in subsequent calls.
 *
 * @param {object} options
 * @param {string} options.url - catalog base URL, with or without trailing slash
 * @param {string} [options.warehouse] - optional warehouse query param sent to /v1/config
 * @param {RequestInit} [options.requestInit] - fetch options (e.g. Authorization header)
 * @returns {Promise<RestCatalogContext>}
 */
export async function restCatalogConnect({ url, warehouse, requestInit }) {
  const base = url.replace(/\/$/, '')
  const configUrl = warehouse
    ? `${base}/v1/config?warehouse=${encodeURIComponent(warehouse)}`
    : `${base}/v1/config`
  const res = await fetch(configUrl, requestInit)
  if (!res.ok) await throwRestError(res)
  const body = await res.json()
  return Object.freeze({
    type: 'rest',
    url: base,
    prefix: typeof body.prefix === 'string' ? body.prefix : '',
    defaults: body.defaults ?? {},
    overrides: body.overrides ?? {},
    requestInit,
  })
}

/**
 * List namespaces. Multi-level namespaces are returned as arrays of strings.
 * Follows pagination (`next-page-token`) until exhausted.
 *
 * @param {RestCatalogContext} ctx
 * @param {object} [options]
 * @param {string | string[]} [options.parent] - parent namespace to scope the listing
 * @returns {Promise<string[][]>}
 */
export function restCatalogListNamespaces(ctx, { parent } = {}) {
  /** @type {Record<string, string>} */
  const params = {}
  if (parent !== undefined) params.parent = encodeNamespace(parent)
  return paginate(params, async query => {
    const res = await restFetch(ctx, `namespaces${query}`)
    const body = await res.json()
    return { items: body.namespaces ?? [], nextPageToken: body['next-page-token'] }
  })
}

/**
 * List tables within a namespace.
 * Follows pagination (`next-page-token`) until exhausted.
 *
 * @param {RestCatalogContext} ctx
 * @param {object} options
 * @param {string | string[]} options.namespace
 * @returns {Promise<TableIdentifier[]>}
 */
export function restCatalogListTables(ctx, { namespace }) {
  const ns = encodeNamespace(namespace)
  return paginate({}, async query => {
    const res = await restFetch(ctx, `namespaces/${ns}/tables${query}`)
    const body = await res.json()
    return { items: body.identifiers ?? [], nextPageToken: body['next-page-token'] }
  })
}

/**
 * Load a single table. Returns the inline TableMetadata, the metadata
 * file location, and any per-table config the server returned.
 *
 * The returned `metadata` and `metadata.location` can be passed directly
 * into `icebergRead({ tableUrl: metadata.location, metadata })`.
 *
 * @param {RestCatalogContext} ctx
 * @param {object} options
 * @param {string | string[]} options.namespace
 * @param {string} options.table
 * @returns {Promise<LoadTableResponse>}
 */
export async function restCatalogLoadTable(ctx, { namespace, table }) {
  const ns = encodeNamespace(namespace)
  const tbl = encodeURIComponent(table)
  const res = await restFetch(ctx, `namespaces/${ns}/tables/${tbl}`)
  const body = await res.json()
  return {
    metadataLocation: body['metadata-location'],
    metadata: /** @type {TableMetadata} */ (body.metadata),
    config: body.config ?? {},
  }
}

/**
 * Load vended storage credentials for a table. The catalog returns
 * per-prefix credential configs (e.g. temporary S3/GCS keys) that callers
 * pass to their resolver/lister to access the table's data files.
 *
 * @param {RestCatalogContext} ctx
 * @param {object} options
 * @param {string | string[]} options.namespace
 * @param {string} options.table
 * @returns {Promise<StorageCredential[]>}
 */
export async function restCatalogLoadCredentials(ctx, { namespace, table }) {
  const ns = encodeNamespace(namespace)
  const tbl = encodeURIComponent(table)
  const res = await restFetch(ctx, `namespaces/${ns}/tables/${tbl}/credentials`)
  const body = await res.json()
  return body['storage-credentials'] ?? []
}

/**
 * Create a new table in the catalog. The server allocates the metadata file
 * and returns the resulting `LoadTableResponse`. When `stageCreate` is true
 * the server stages the create without committing it, so the caller can
 * follow up with an `updateTable` commit.
 *
 * @param {RestCatalogContext} ctx
 * @param {object} options
 * @param {string | string[]} options.namespace
 * @param {string} options.table
 * @param {Schema} options.schema
 * @param {string} [options.location]
 * @param {PartitionSpec} [options.partitionSpec]
 * @param {SortOrder} [options.writeOrder]
 * @param {boolean} [options.stageCreate]
 * @param {Record<string, string>} [options.properties]
 * @returns {Promise<LoadTableResponse>}
 */
export async function restCatalogCreateTable(ctx, {
  namespace,
  table,
  schema,
  location,
  partitionSpec,
  writeOrder,
  stageCreate,
  properties,
}) {
  const ns = encodeNamespace(namespace)
  /** @type {Record<string, unknown>} */
  const body = { name: table, schema }
  if (location !== undefined) body.location = location
  if (partitionSpec !== undefined) body['partition-spec'] = partitionSpec
  if (writeOrder !== undefined) body['write-order'] = writeOrder
  if (stageCreate !== undefined) body['stage-create'] = stageCreate
  if (properties !== undefined) body.properties = properties
  const res = await restFetch(ctx, `namespaces/${ns}/tables`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  const responseBody = await res.json()
  return {
    metadataLocation: responseBody['metadata-location'],
    metadata: /** @type {TableMetadata} */ (responseBody.metadata),
    config: responseBody.config ?? {},
  }
}

/**
 * Register an existing metadata file as a table in the catalog. The catalog
 * does not write any files; it only records a pointer to the supplied
 * `metadataLocation`. Some servers honor `overwrite` to replace an existing
 * table entry; others reject it as `AlreadyExistsException`.
 *
 * @param {RestCatalogContext} ctx
 * @param {object} options
 * @param {string | string[]} options.namespace
 * @param {string} options.table
 * @param {string} options.metadataLocation
 * @param {boolean} [options.overwrite]
 * @returns {Promise<LoadTableResponse>}
 */
export async function restCatalogRegisterTable(ctx, { namespace, table, metadataLocation, overwrite }) {
  const ns = encodeNamespace(namespace)
  /** @type {Record<string, unknown>} */
  const body = { name: table, 'metadata-location': metadataLocation }
  if (overwrite !== undefined) body.overwrite = overwrite
  const res = await restFetch(ctx, `namespaces/${ns}/register`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  const responseBody = await res.json()
  return {
    metadataLocation: responseBody['metadata-location'],
    metadata: /** @type {TableMetadata} */ (responseBody.metadata),
    config: responseBody.config ?? {},
  }
}

/**
 * Commit updates to a table. Sends `requirements` and `updates` to the
 * catalog's `commit` endpoint; the server applies the updates atomically iff
 * every requirement still holds against the current metadata, otherwise it
 * responds with `CommitFailedException`. Returns the committed metadata and
 * its new location.
 *
 * @param {RestCatalogContext} ctx
 * @param {object} options
 * @param {string | string[]} options.namespace
 * @param {string} options.table
 * @param {TableRequirement[]} options.requirements
 * @param {TableUpdate[]} options.updates
 * @returns {Promise<LoadTableResponse>}
 */
export async function restCatalogUpdateTable(ctx, { namespace, table, requirements, updates }) {
  const ns = encodeNamespace(namespace)
  const tbl = encodeURIComponent(table)
  const res = await restFetch(ctx, `namespaces/${ns}/tables/${tbl}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ requirements, updates }),
  })
  const responseBody = await res.json()
  return {
    metadataLocation: responseBody['metadata-location'],
    metadata: /** @type {TableMetadata} */ (responseBody.metadata),
    config: responseBody.config ?? {},
  }
}

/**
 * Drop a table from the catalog. The optional `purgeRequested` flag asks the
 * server to also delete the table's data and metadata files; servers may
 * ignore it for managed tables. Resolves on a 2xx response, otherwise throws.
 *
 * @param {RestCatalogContext} ctx
 * @param {object} options
 * @param {string | string[]} options.namespace
 * @param {string} options.table
 * @param {boolean} [options.purgeRequested]
 * @returns {Promise<void>}
 */
export async function restCatalogDropTable(ctx, { namespace, table, purgeRequested }) {
  const ns = encodeNamespace(namespace)
  const tbl = encodeURIComponent(table)
  const query = purgeRequested ? '?purgeRequested=true' : ''
  await restFetch(ctx, `namespaces/${ns}/tables/${tbl}${query}`, { method: 'DELETE' })
}

/**
 * Create a namespace. Returns the namespace as the server stored it (which may
 * include defaulted properties).
 *
 * @param {RestCatalogContext} ctx
 * @param {object} options
 * @param {string | string[]} options.namespace
 * @param {Record<string, string>} [options.properties]
 * @returns {Promise<{namespace: string[], properties: Record<string, string>}>}
 */
export async function restCatalogCreateNamespace(ctx, { namespace, properties }) {
  const ns = Array.isArray(namespace) ? namespace : namespace.split('.')
  const res = await restFetch(ctx, 'namespaces', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ namespace: ns, properties: properties ?? {} }),
  })
  const body = await res.json()
  return {
    namespace: body.namespace ?? ns,
    properties: body.properties ?? {},
  }
}

/**
 * Drop a namespace. Resolves on a 2xx response, otherwise throws.
 *
 * @param {RestCatalogContext} ctx
 * @param {object} options
 * @param {string | string[]} options.namespace
 * @returns {Promise<void>}
 */
export async function restCatalogDropNamespace(ctx, { namespace }) {
  const ns = encodeNamespace(namespace)
  await restFetch(ctx, `namespaces/${ns}`, { method: 'DELETE' })
}

/**
 * Rename a table. Both `source` and `destination` are full table identifiers;
 * the server may reject cross-namespace renames depending on its policy.
 *
 * @param {RestCatalogContext} ctx
 * @param {object} options
 * @param {TableIdentifier} options.source
 * @param {TableIdentifier} options.destination
 * @returns {Promise<void>}
 */
export async function restCatalogRenameTable(ctx, { source, destination }) {
  await restFetch(ctx, 'tables/rename', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ source, destination }),
  })
}

/**
 * Encode a namespace for use in a URL path segment.
 * Multi-level namespaces are joined with the unit separator (%1F) per spec.
 * Accepts either a dot-separated string ('db.sub') or an array (['db','sub']).
 *
 * @param {string | string[]} namespace
 * @returns {string}
 */
function encodeNamespace(namespace) {
  const parts = Array.isArray(namespace) ? namespace : namespace.split('.')
  return parts.map(p => encodeURIComponent(p)).join('%1F')
}

/**
 * Issue a request against the catalog, prepending /v1/{prefix?}/ and
 * merging the context's requestInit with per-call init.
 *
 * @param {RestCatalogContext} ctx
 * @param {string} path - path after /v1/{prefix}/
 * @param {RequestInit} [init]
 * @returns {Promise<Response>}
 */
async function restFetch(ctx, path, init) {
  const prefixSegment = ctx.prefix ? `${ctx.prefix.replace(/^\/|\/$/g, '')}/` : ''
  const fullUrl = `${ctx.url}/v1/${prefixSegment}${path}`
  const merged = mergeRequestInit(ctx.requestInit, init)
  const res = await fetch(fullUrl, merged)
  if (!res.ok) await throwRestError(res)
  return res
}

/**
 * Merge two RequestInit objects, combining headers.
 *
 * @param {RequestInit} [a]
 * @param {RequestInit} [b]
 * @returns {RequestInit | undefined}
 */
function mergeRequestInit(a, b) {
  if (!a) return b
  if (!b) return a
  return {
    ...a,
    ...b,
    headers: { ...headersToObject(a.headers), ...headersToObject(b.headers) },
  }
}

/**
 * Normalize HeadersInit to a plain object.
 *
 * @param {HeadersInit} [h]
 * @returns {Record<string, string>}
 */
function headersToObject(h) {
  if (!h) return {}
  if (h instanceof Headers) {
    /** @type {Record<string, string>} */
    const out = {}
    h.forEach((v, k) => { out[k] = v })
    return out
  }
  if (Array.isArray(h)) return Object.fromEntries(h)
  return /** @type {Record<string, string>} */ (h)
}

/**
 * Read an ErrorModel response and throw a descriptive Error.
 *
 * @param {Response} res
 * @returns {Promise<never>}
 */
async function throwRestError(res) {
  let detail = ''
  try {
    const body = await res.json()
    if (body?.error) {
      const { code, type, message } = body.error
      detail = `${code ?? res.status} ${type ?? ''}: ${message ?? ''}`.trim()
    }
  } catch { /* not JSON */ }
  throw new Error(detail || `${res.status} ${res.statusText}`)
}

/**
 * Walk through paginated responses, concatenating items.
 *
 * @template T
 * @param {Record<string, string>} baseParams - query params applied to every page
 * @param {(query: string) => Promise<{items: T[], nextPageToken?: string}>} fetchPage
 * @returns {Promise<T[]>}
 */
async function paginate(baseParams, fetchPage) {
  /** @type {T[]} */
  const out = []
  let pageToken
  while (true) {
    const params = { ...baseParams }
    if (pageToken) params.pageToken = pageToken
    const keys = Object.keys(params)
    const query = keys.length
      ? '?' + keys.map(k => `${k}=${params[k]}`).join('&')
      : ''
    const { items, nextPageToken } = await fetchPage(query)
    out.push(...items)
    if (!nextPageToken) return out
    pageToken = encodeURIComponent(nextPageToken)
  }
}
