import { fetchManifestEntries } from '../manifest.js'
import { uuid4 } from '../utils.js'
import { canRewriteSpec, partitionSchema, targetSizeProperty, writeManifestFile } from './rewrite-manifests.js'

/**
 * Manifest merging on commit, following Java's `MergeAppend` /
 * `ManifestMergeManager` semantics.
 *
 * @import {Manifest, ManifestEntry, Resolver, TableMetadata} from '../../src/types.js'
 */

const DEFAULT_MIN_COUNT_TO_MERGE = 100

/**
 * Resolve the `commit.manifest*` table properties, falling back to Java's
 * defaults: merging enabled, min count 100, target size 8 MB.
 *
 * @param {Record<string, string> | undefined} properties
 * @returns {{ enabled: boolean, minCountToMerge: number, targetSizeBytes: number }}
 */
export function manifestMergeConfig(properties) {
  const enabled = properties?.['commit.manifest-merge.enabled']
  const minCount = Number(properties?.['commit.manifest.min-count-to-merge'])
  return {
    enabled: enabled === undefined || String(enabled).toLowerCase() !== 'false',
    minCountToMerge: Number.isFinite(minCount) && minCount > 0 ? minCount : DEFAULT_MIN_COUNT_TO_MERGE,
    targetSizeBytes: targetSizeProperty(properties),
  }
}

/**
 * Greedily pack manifests, in order, into bins of at most `targetSizeBytes`
 * by `manifest_length` (Java `ListPacker` with lookback 1). A manifest larger
 * than the target gets a bin of its own. The manifest list is ordered oldest
 * first, so packing from the front leaves the under-filled bin at the end,
 * holding the newest manifests: Java's `packEnd` over its newest-first list.
 *
 * @param {Manifest[]} manifests
 * @param {number} targetSizeBytes
 * @returns {Manifest[][]}
 */
export function packManifests(manifests, targetSizeBytes) {
  /** @type {Manifest[][]} */
  const bins = []
  /** @type {Manifest[]} */
  let bin = []
  let binSize = 0
  for (const manifest of manifests) {
    const size = Number(manifest.manifest_length)
    if (bin.length && binSize + size > targetSizeBytes) {
      bins.push(bin)
      bin = []
      binSize = 0
    }
    bin.push(manifest)
    binSize += size
  }
  if (bin.length) bins.push(bin)
  return bins
}

/**
 * Merge a snapshot's manifest list per Java's `ManifestMergeManager`. Data and
 * delete manifests are merged independently and never with each other; within
 * a content type manifests are grouped by partition spec and packed into
 * target-size bins. A single-manifest bin is kept as-is. The bin holding the
 * newest manifest of its content type is kept as-is while it has fewer than
 * `minCountToMerge` manifests, so small commits stay cheap; every other bin is
 * rewritten into one manifest.
 *
 * Rewritten entries follow Java: ADDED entries of the committing snapshot stay
 * ADDED (inheriting its sequence number), DELETED entries of the committing
 * snapshot stay DELETED, older DELETED entries are dropped, and everything
 * else becomes EXISTING with its original snapshot id and data / file sequence
 * numbers. Bins are processed one at a time so only one bin's entries are held
 * in memory.
 *
 * Call this once per commit attempt against that attempt's base: a merge
 * computed against a stale base would drop manifests committed concurrently.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata - Base metadata of this attempt.
 * @param {Resolver} options.resolver - Resolver with a writer method.
 * @param {Manifest[]} options.manifests - Full manifest list, oldest first.
 * @param {bigint} options.snapshotId - The committing snapshot.
 * @param {bigint} options.sequenceNumber - The committing sequence number.
 * @param {2|3} options.formatVersion
 * @param {{ minCountToMerge: number, targetSizeBytes: number }} options.config
 * @returns {Promise<{ manifests: Manifest[], writtenFiles: string[] }>}
 */
export async function mergeManifests({
  tableUrl, metadata, resolver, manifests, snapshotId, sequenceNumber, formatVersion, config,
}) {
  const mergeUuid = uuid4()
  // Each merged bin is emitted where its newest member sat and its other
  // members are dropped, so manifests that stay keep their list order.
  /** @type {Map<Manifest, Manifest | null>} */
  const replacements = new Map()
  /** @type {string[]} */
  const writtenFiles = []

  for (const content of /** @type {(0|1)[]} */ ([0, 1])) {
    const ofContent = manifests.filter(m => (m.content ?? 0) === content)
    if (!ofContent.length) continue
    const newest = ofContent[ofContent.length - 1]

    /** @type {Map<number, Manifest[]>} */
    const groups = new Map()
    for (const manifest of ofContent) {
      const specId = manifest.partition_spec_id ?? 0
      let group = groups.get(specId)
      if (!group) groups.set(specId, group = [])
      group.push(manifest)
    }

    for (const [specId, group] of groups) {
      const spec = metadata['partition-specs'].find(s => s['spec-id'] === specId)
      const schema = spec && partitionSchema(metadata, spec)
      // A spec we cannot re-encode losslessly is carried through unmerged.
      if (!spec || !schema || !canRewriteSpec(schema, spec)) continue
      for (const bin of packManifests(group, config.targetSizeBytes)) {
        if (bin.length === 1 || bin.includes(newest) && bin.length < config.minCountToMerge) continue
        /** @type {ManifestEntry[]} */
        const entries = []
        const binEntries = await Promise.all(bin.map(m => fetchManifestEntries(m, resolver)))
        for (const manifestEntries of binEntries) {
          for (const entry of manifestEntries) {
            const carried = carryEntry(entry, snapshotId)
            if (carried) entries.push(carried)
          }
        }
        const manifestPath = `${tableUrl}/metadata/${mergeUuid}-m${writtenFiles.length}.avro`
        const merged = await writeManifestFile({
          resolver, manifestPath, schema, spec, content, entries, snapshotId, sequenceNumber, formatVersion,
        })
        writtenFiles.push(manifestPath)
        for (const manifest of bin) replacements.set(manifest, null)
        replacements.set(bin[bin.length - 1], merged)
      }
    }
  }
  if (!replacements.size) return { manifests, writtenFiles }
  /** @type {Manifest[]} */
  const output = []
  for (const manifest of manifests) {
    const replacement = replacements.get(manifest)
    if (replacement === undefined) output.push(manifest)
    else if (replacement) output.push(replacement)
  }
  return { manifests: output, writtenFiles }
}

/**
 * Decide the status an entry carries into a rewritten manifest, or null to
 * drop it (a DELETED tombstone from an earlier snapshot).
 *
 * @param {ManifestEntry} entry
 * @param {bigint} snapshotId
 * @returns {ManifestEntry | null}
 */
function carryEntry(entry, snapshotId) {
  const ownSnapshot = entry.snapshot_id != null && BigInt(entry.snapshot_id) === snapshotId
  if (entry.status === 2) return ownSnapshot ? entry : null
  if (entry.status === 1 && ownSnapshot) return entry
  return { ...entry, status: 0 }
}
