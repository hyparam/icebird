import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { isDeepStrictEqual } from 'node:util'
import { asyncBufferFromFile } from 'hyparquet/src/node.js'

const currentRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const options = parseArguments(process.argv.slice(2))
if (!options.baseline) {
  throw new Error('Pass --baseline <icebird-v0.8.22-directory>')
}
if (options.tables.length === 0) {
  throw new Error('Pass at least one --table <hypaware-iceberg-table-directory>')
}

const querySuite = makeQueries(options.since)
const benchmarks = options.query
  ? querySuite.filter(function selectedQuery(benchmark) {
    return benchmark.name.toLowerCase().includes(options.query.toLowerCase())
  })
  : querySuite
if (benchmarks.length === 0) throw new Error(`No benchmark matched "${options.query}"`)

const baseline = await loadVersion('baseline', resolve(options.baseline), options.tables)
const proposed = await loadVersion('proposed', currentRoot, options.tables)
const versions = { baseline, proposed }

console.log(JSON.stringify({
  type: 'environment',
  baseline: baseline.version,
  proposed: proposed.version,
  tables: options.tables,
  rows: { baseline: baseline.source.numRows, proposed: proposed.source.numRows },
  queries: benchmarks.length,
  iterations: options.iterations,
}))

/** @type {Array<{name: string, speedup: number}>} */
const headline = []
for (const benchmark of benchmarks) {
  const baselineRows = await runQuery(baseline, benchmark.query)
  const proposedRows = await runQuery(proposed, benchmark.query)
  if (!isDeepStrictEqual(baselineRows, proposedRows)) {
    console.error(JSON.stringify({
      type: 'mismatch',
      name: benchmark.name,
      baselineRows,
      proposedRows,
    }, jsonReplacer, 2))
    throw new Error(`Result mismatch for ${benchmark.name}`)
  }

  /** @type {Record<'baseline' | 'proposed', Measurement[]>} */
  const measurements = { baseline: [], proposed: [] }
  for (let iteration = 0; iteration < options.iterations; iteration++) {
    const order = iteration % 2 === 0
      ? /** @type {const} */ (['baseline', 'proposed'])
      : /** @type {const} */ (['proposed', 'baseline'])
    for (const name of order) {
      measurements[name].push(await measure(versions[name], benchmark.query))
    }
  }
  const baselineSummary = summarize(measurements.baseline)
  const proposedSummary = summarize(measurements.proposed)
  const speedup = baselineSummary.medianMs / proposedSummary.medianMs
  headline.push({ name: benchmark.name, speedup })
  console.log(JSON.stringify({
    type: 'result',
    name: benchmark.name,
    category: benchmark.category,
    resultRows: baselineRows.length,
    baseline: baselineSummary,
    proposed: proposedSummary,
    speedup,
  }))
}

console.log(JSON.stringify({
  type: 'headline',
  geometricMeanSpeedup: geometricMean(headline.map(function speedup(result) { return result.speedup })),
  fastest: [...headline].sort(function descending(a, b) { return b.speedup - a.speedup })[0],
  slowest: [...headline].sort(function ascending(a, b) { return a.speedup - b.speedup })[0],
}))

/**
 * Queries are derived from Hypaware's recent direct SQL history and built-in
 * overview panels. The four overview statements are preserved verbatim in
 * shape; retrieval probes exercise the wide deferred columns that motivated
 * Icebird's native prepared scanner.
 *
 * @param {string} since
 * @returns {Array<{name: string, category: string, query: string}>}
 */
function makeQueries(since) {
  return [
    {
      name: 'overview date counts',
      category: 'overview',
      query: 'SELECT date, COUNT(*) n FROM messages GROUP BY 1 ORDER BY 1 DESC',
    },
    {
      name: 'overview tool calls and sessions',
      category: 'overview',
      query: `SELECT tool_name, COUNT(*) calls, COUNT(DISTINCT session_id) sessions
        FROM messages WHERE date >= '${since}'
        AND part_type = 'tool_call' AND tool_name IS NOT NULL
        GROUP BY 1 ORDER BY calls DESC LIMIT 10`,
    },
    {
      name: 'overview provider model tokens',
      category: 'overview',
      query: `SELECT provider, model,
        COALESCE(SUM(CAST(JSON_EXTRACT(attributes, '$.usage.input_tokens') AS BIGINT)), 0) input_tokens,
        COALESCE(SUM(COALESCE(CAST(JSON_EXTRACT(attributes, '$.usage.cache_read_tokens') AS BIGINT), 0)
          + COALESCE(CAST(JSON_EXTRACT(attributes, '$.usage.cache_write_tokens') AS BIGINT), 0)), 0) cached_tokens,
        COALESCE(SUM(CAST(JSON_EXTRACT(attributes, '$.usage.output_tokens') AS BIGINT)), 0) output_tokens
        FROM messages WHERE date >= '${since}'
        GROUP BY 1, 2 ORDER BY input_tokens + output_tokens DESC`,
    },
    {
      name: 'overview daily sessions and tokens',
      category: 'overview',
      query: `SELECT date, COUNT(DISTINCT session_id) sessions,
        COALESCE(SUM(CAST(JSON_EXTRACT(attributes, '$.usage.input_tokens') AS BIGINT)), 0) input_tokens,
        COALESCE(SUM(CAST(JSON_EXTRACT(attributes, '$.usage.output_tokens') AS BIGINT)), 0) output_tokens
        FROM messages WHERE date >= '${since}'
        GROUP BY 1 ORDER BY 1 DESC`,
    },
    {
      name: 'selective tool result payload',
      category: 'retrieval',
      query: `SELECT content_text FROM messages
        WHERE part_type = 'tool_result' AND content_text IS NOT NULL LIMIT 100`,
    },
    {
      name: 'recent attributes preview',
      category: 'retrieval',
      query: `SELECT attributes FROM messages
        WHERE part_type = 'tool_call' AND attributes IS NOT NULL LIMIT 100`,
    },
    {
      name: 'largest system prompts',
      category: 'top-k',
      query: `SELECT session_id, LENGTH(system_text) system_chars FROM messages
        WHERE system_text IS NOT NULL ORDER BY system_chars DESC LIMIT 20`,
    },
  ]
}

/**
 * @typedef {object} BenchmarkOptions
 * @property {string} [baseline]
 * @property {string[]} tables
 * @property {number} iterations
 * @property {string} since
 * @property {string} [query]
 */

/**
 * @param {string[]} args
 * @returns {BenchmarkOptions}
 */
function parseArguments(args) {
  /** @type {BenchmarkOptions} */
  const parsed = { tables: [], iterations: 3, since: '2026-07-17' }
  for (let index = 0; index < args.length; index++) {
    const flag = args[index]
    const value = args[++index]
    if (!value) throw new Error(`Missing value for ${flag}`)
    if (flag === '--baseline') parsed.baseline = value
    else if (flag === '--table') parsed.tables.push(resolve(value))
    else if (flag === '--iterations') parsed.iterations = Number(value)
    else if (flag === '--since') parsed.since = value
    else if (flag === '--query') parsed.query = value
    else throw new Error(`Unknown argument ${flag}`)
  }
  if (!Number.isInteger(parsed.iterations) || parsed.iterations < 1) {
    throw new Error('--iterations must be a positive integer')
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(parsed.since)) {
    throw new Error('--since must use YYYY-MM-DD')
  }
  return parsed
}

/**
 * @typedef {object} BenchmarkVersion
 * @property {string} name
 * @property {string} version
 * @property {any} source
 * @property {Function} executeSql
 * @property {Function} collect
 */

/**
 * @param {string} name
 * @param {string} root
 * @param {string[]} tables
 * @returns {Promise<BenchmarkVersion>}
 */
async function loadVersion(name, root, tables) {
  const icebird = await import(moduleUrl(resolve(root, 'src/sql/icebergDataSource.js')))
  const engine = await import(moduleUrl(resolve(root, 'node_modules/squirreling/src/index.js')))
  const packageJson = JSON.parse(await readFile(resolve(root, 'package.json'), 'utf8'))
  const dependency = packageJson.dependencies.squirreling
  const installedVersion = await squirrelingVersion(root)
  const resolver = {
    reader(path) {
      return asyncBufferFromFile(path.startsWith('file://') ? fileURLToPath(path) : path)
    },
  }
  const partitions = []
  for (const table of tables) {
    const metadata = await loadTableMetadata(table)
    partitions.push(await icebird.icebergDataSource({
      tableUrl: metadata.location,
      metadata,
      resolver,
    }))
  }
  return {
    name,
    version: dependency === installedVersion
      ? `${packageJson.version} / squirreling ${installedVersion}`
      : `${packageJson.version} / squirreling ${dependency} (package ${installedVersion})`,
    source: unionDataSource(partitions),
    executeSql: engine.executeSql,
    collect: engine.collect,
  }
}

/**
 * @param {string} root
 * @returns {Promise<string>}
 */
async function squirrelingVersion(root) {
  const packageJson = JSON.parse(await readFile(resolve(root, 'node_modules/squirreling/package.json'), 'utf8'))
  return packageJson.version
}

/**
 * @param {string} table
 * @returns {Promise<any>}
 */
async function loadTableMetadata(table) {
  const version = (await readFile(resolve(table, 'metadata/version-hint.text'), 'utf8')).trim()
  return JSON.parse(await readFile(resolve(table, `metadata/v${version}.metadata.json`), 'utf8'))
}

/**
 * Combine Hypaware's per-source Iceberg tables without materializing them.
 * Legacy scans retain the v0.8.22 behavior; prepared scans concatenate native
 * batches and leave global filtering/LIMIT/OFFSET to Squirreling.
 *
 * @param {any[]} partitions
 * @returns {any}
 */
function unionDataSource(partitions) {
  if (partitions.length === 0) throw new Error('No table partitions loaded')
  const first = partitions[0]
  const source = {
    columns: first.columns,
    numRows: partitions.every(function hasRows(partition) { return partition.numRows !== undefined })
      ? partitions.reduce(function totalRows(total, partition) { return total + partition.numRows }, 0)
      : undefined,
    scan(options) {
      const scans = partitions.map(function partitionScan(partition) {
        return partition.scan({ ...options, limit: undefined, offset: undefined })
      })
      return {
        appliedWhere: scans.every(function applied(scan) { return scan.appliedWhere }),
        appliedLimitOffset: false,
        async *rows() {
          for (const scan of scans) yield* scan.rows()
        },
      }
    },
  }
  if (first.schema && partitions.every(function prepared(partition) { return partition.prepareScan })) {
    source.schema = first.schema
    source.prepareScan = function prepareScan(request) {
      const prepared = partitions.map(function preparePartition(partition) {
        return partition.prepareScan({ ...request, limit: undefined, offset: undefined })
      })
      return {
        schema: prepared[0].schema,
        residual: {
          filter: request.filter,
          limit: request.limit,
          offset: request.offset,
        },
        properties: {
          exactRows: request.filter ? undefined : source.numRows,
          maxRows: prepared.reduce(function totalRows(total, scan) {
            return total + (scan.properties.maxRows ?? 0)
          }, 0),
        },
        async *batches(batchOptions) {
          for (const scan of prepared) yield* scan.batches(batchOptions)
        },
      }
    }
  }
  return source
}

/**
 * @param {BenchmarkVersion} version
 * @param {string} query
 * @returns {Promise<Record<string, unknown>[]>}
 */
function runQuery(version, query) {
  return version.collect(version.executeSql({ tables: { messages: version.source }, query }))
}

/**
 * @typedef {object} Measurement
 * @property {number} ms
 * @property {number} peakHeapGrowthMb
 */

/**
 * @param {BenchmarkVersion} version
 * @param {string} query
 * @returns {Promise<Measurement>}
 */
async function measure(version, query) {
  globalThis.gc?.()
  const before = process.memoryUsage().heapUsed
  let peak = before
  const sampler = setInterval(function sampleHeap() {
    peak = Math.max(peak, process.memoryUsage().heapUsed)
  }, 5)
  const start = performance.now()
  try {
    await runQuery(version, query)
  } finally {
    clearInterval(sampler)
  }
  const ms = performance.now() - start
  peak = Math.max(peak, process.memoryUsage().heapUsed)
  return { ms, peakHeapGrowthMb: (peak - before) / 1048576 }
}

/**
 * @param {Measurement[]} measurements
 * @returns {{medianMs: number, minMs: number, medianPeakHeapGrowthMb: number}}
 */
function summarize(measurements) {
  return {
    medianMs: median(measurements.map(function milliseconds(value) { return value.ms })),
    minMs: Math.min(...measurements.map(function milliseconds(value) { return value.ms })),
    medianPeakHeapGrowthMb: median(measurements.map(function heap(value) { return value.peakHeapGrowthMb })),
  }
}

/**
 * @param {number[]} values
 * @returns {number}
 */
function median(values) {
  const sorted = [...values].sort(function ascending(a, b) { return a - b })
  const midpoint = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 0
    ? (sorted[midpoint - 1] + sorted[midpoint]) / 2
    : sorted[midpoint]
}

/**
 * @param {number[]} values
 * @returns {number}
 */
function geometricMean(values) {
  return Math.exp(values.reduce(function sum(total, value) { return total + Math.log(value) }, 0) / values.length)
}

/**
 * @param {string} path
 * @returns {string}
 */
function moduleUrl(path) {
  return pathToFileURL(path).href
}

/**
 * @param {string} _key
 * @param {unknown} value
 * @returns {unknown}
 */
function jsonReplacer(_key, value) {
  return typeof value === 'bigint' ? value.toString() : value
}
