import { readBatchColumn, selectedRowCount, valueAt } from 'squirreling'
import { deserializeValue } from '../write/serde.js'

/**
 * @import {AsyncBatch, Field, ScanTopK} from 'squirreling'
 * @import {IcebergType, ManifestEntry, Schema, TopKRow} from '../../src/types.js'
 */

/**
 * Compute a conservative Kth-value threshold from file bounds and counts.
 * For DESC, a file with N rows and lower bound L certifies N rows >= L.
 * The weighted Kth greatest lower bound therefore bounds the actual Kth row
 * from below. Only files whose upper bounds are STRICTLY below it can go.
 * ASC is symmetric. Equal bounds remain, preserving ties and input order.
 * Unknown/null-bearing files remain and contribute no certified rows.
 * Position deletes may supply exact surviving counts: the original bounds
 * still enclose every surviving value. Callers must disable this optimization
 * for equality deletes or row filters whose surviving counts are unknown.
 *
 * @param {ManifestEntry[]} entries
 * @param {Schema} schema
 * @param {ScanTopK} hint
 * @param {(entry: ManifestEntry) => bigint} [rowCount] - Exact surviving rows; defaults to the physical record count.
 * @returns {ManifestEntry[]}
 */
export function pruneTopKFiles(entries, schema, hint, rowCount) {
  if (hint.orderBy.length !== 1) return entries
  const term = hint.orderBy[0]
  const descending = term.direction === 'DESC'
  const field = schema.fields.find(f => f.id === term.field)
  if (!field || typeof field.type !== 'string' ||
      !['int', 'long', 'date', 'timestamp', 'timestamptz', 'timestamp_ns', 'timestamptz_ns', 'string'].includes(field.type)) return entries
  const fieldType = field.type
  const counts = entries.map(entry => rowCount ? rowCount(entry) : entry.data_file.record_count)
  const stats = entries.map((entry, i) => {
    const count = counts[i]
    if (count <= 0n) return undefined
    const bounds = fileBounds(entry, field.id, fieldType)
    return bounds && { count, ...bounds }
  })
  const known = stats.filter(s => s !== undefined)
  const direction = descending ? -1 : 1
  known.sort((a, b) => {
    const av = descending ? a.lower : a.upper
    const bv = descending ? b.lower : b.upper
    return direction * (av < bv ? -1 : av > bv ? 1 : 0)
  })
  let remaining = BigInt(hint.limit)
  for (const stat of known) {
    remaining -= BigInt(stat.count)
    if (remaining > 0n) continue
    const threshold = descending ? stat.lower : stat.upper
    return entries.filter((entry, i) => {
      if (counts[i] === 0n) return false
      const bound = stats[i]
      return !bound || (descending ? bound.upper >= threshold : bound.lower <= threshold)
    })
  }
  return entries.filter((entry, i) => counts[i] !== 0n)
}

/**
 * Read promising files once, keeping the best K actual matches. Unknown bounds
 * remain eligible; only a strictly worse optimistic bound can skip a file.
 * The reader must apply the complete predicate and deletes before yielding.
 *
 * Heap rows carry original positions, so reading files out of order cannot
 * change stable ties. Emit the winning prefix in original input order for the
 * engine's final sort/offset. Payloads are gathered for batch winners before
 * advancing: keep O(K * projected width) values, not K pinned Parquet buffers.
 * The scan contract requires every boundary tie. If a discarded tie remains
 * at the final cutoff, stream eligible files again in physical order instead
 * of buffering an unbounded number of tied payloads. Unsupported hints return
 * undefined and use the ordinary streaming scan.
 *
 * @param {ManifestEntry[]} entries
 * @param {Schema} schema
 * @param {ScanTopK} hint
 * @param {readonly Field[]} fields
 * @param {(entry: ManifestEntry, fields: readonly Field[]) => AsyncIterable<AsyncBatch>} read
 * @param {AbortSignal} [signal]
 * @returns {AsyncGenerator<AsyncBatch> | undefined}
 */
export function scanTopKFiles(entries, schema, hint, fields, read, signal) {
  if (hint.orderBy.length !== 1 || !Number.isSafeInteger(hint.limit) || hint.limit <= 0) return
  const term = hint.orderBy[0]
  const field = schema.fields.find(f => f.id === term.field)
  if (!field || typeof field.type !== 'string' ||
      !['int', 'long', 'date', 'timestamp', 'timestamptz', 'timestamp_ns', 'timestamptz_ns', 'string'].includes(field.type)) return
  const descending = term.direction === 'DESC'
  const candidates = entries.map((entry, index) => {
    const bounds = fileBounds(entry, field.id, field.type)
    return { entry, index, best: bounds && (descending ? bounds.upper : bounds.lower) }
  })
  candidates.sort((a, b) => {
    if (a.best === undefined) return b.best === undefined ? a.index - b.index : 1
    if (b.best === undefined) return -1
    return compareValue(a.best, b.best) || a.index - b.index
  })
  let keyIndex = fields.findIndex(f => f.id === field.id)
  const readFields = [...fields]
  if (keyIndex < 0) {
    keyIndex = readFields.length
    readFields.push({ id: field.id, name: field.name, dataType: { type: 'unknown' }, nullable: !field.required })
  }
  return batches()

  /**
   * Compare SQL sort values in requested order, including explicit null order.
   * Date values have already been reduced to SQL's millisecond precision.
   * @param {TopKRow['value']} a
   * @param {TopKRow['value']} b
   * @returns {number}
   */
  function compareValue(a, b) {
    if (a === null || b === null) {
      if (a === b) return 0
      return (a === null ? -1 : 1) * (term.nulls === 'LAST' ? -1 : 1)
    }
    return (a < b ? -1 : a > b ? 1 : 0) * (descending ? -1 : 1)
  }

  /**
   * @param {TopKRow} a
   * @param {TopKRow} b
   * @returns {number}
   */
  function compareRows(a, b) {
    return compareValue(a.value, b.value) || comparePosition(a, b)
  }

  /** @returns {AsyncGenerator<AsyncBatch>} */
  async function* batches() {
    /** @type {TopKRow[]} */
    const heap = []
    /** @type {TopKRow['value'] | undefined} */
    let omittedTie
    for (const { entry, index, best } of candidates) {
      signal?.throwIfAborted()
      // Equal bounds stay: an earlier physical row may win the boundary tie.
      if (heap.length === hint.limit && best !== undefined && compareValue(best, heap[0].value) > 0) continue
      let batchIndex = 0
      for await (const batch of read(entry, readFields)) {
        signal?.throwIfAborted()
        const keys = await readBatchColumn({ batch, columnIndex: keyIndex, signal })
        signal?.throwIfAborted()
        /** @type {Set<TopKRow>} */
        const pending = new Set()
        const count = selectedRowCount(batch.selection)
        for (let i = 0; i < count; i++) {
          if ((i & 1023) === 0) signal?.throwIfAborted()
          const raw = valueAt(keys, i)
          const value = /** @type {TopKRow['value']} */ (raw instanceof Date ? raw.getTime() : raw ?? null)
          const { selection } = batch
          const rowIndex = selection.type === 'all' ? i
            : selection.type === 'range' ? selection.start + i : selection.indices[i]
          if (heap.length === hint.limit) {
            const worst = heap[0]
            const order = compareValue(value, worst.value) || index - worst.entryIndex ||
              batchIndex - worst.batchIndex || rowIndex - worst.rowIndex
            if (order >= 0) {
              if (compareValue(value, worst.value) === 0) omittedTie = value
              continue
            }
          }
          const row = { value, entryIndex: index, batchIndex, rowIndex, values: [] }
          pending.add(row)
          if (heap.length < hint.limit) {
            heap.push(row)
            let child = heap.length - 1
            while (child > 0) {
              const parent = child - 1 >>> 1
              if (compareRows(heap[parent], row) >= 0) break
              heap[child] = heap[parent]
              child = parent
            }
            heap[child] = row
          } else {
            const worst = heap[0]
            pending.delete(worst)
            heap[0] = row
            let parent = 0
            while (parent * 2 + 1 < heap.length) {
              let child = parent * 2 + 1
              if (child + 1 < heap.length && compareRows(heap[child + 1], heap[child]) > 0) child++
              if (compareRows(row, heap[child]) >= 0) break
              heap[parent] = heap[child]
              parent = child
            }
            heap[parent] = row
            if (compareValue(worst.value, heap[0].value) === 0) omittedTie = worst.value
          }
        }
        // Read projected payload only for rows from this batch still in the
        // heap. Copy scalar values so discarded batches/files can be collected.
        const kept = [...pending].sort((a, b) => a.rowIndex - b.rowIndex)
        if (kept.length) {
          const selection = { type: /** @type {const} */ ('indices'),
            indices: Uint32Array.from(kept, row => row.rowIndex), length: batch.selection.length }
          for (let columnIndex = 0; columnIndex < fields.length; columnIndex++) {
            signal?.throwIfAborted()
            const vector = await readBatchColumn({ batch, columnIndex, selection, signal })
            for (let i = 0; i < kept.length; i++) kept[i].values.push(valueAt(vector, i))
          }
        }
        batchIndex++
      }
    }
    signal?.throwIfAborted()
    if (omittedTie !== undefined && heap.length === hint.limit && compareValue(omittedTie, heap[0].value) === 0) {
      const threshold = heap[0].value
      heap.length = 0
      candidates.sort((a, b) => a.index - b.index)
      for (const { entry, best } of candidates) {
        signal?.throwIfAborted()
        if (best !== undefined && compareValue(best, threshold) > 0) continue
        yield* read(entry, fields)
      }
      return
    }
    heap.sort(comparePosition)
    // Bound output vectors even when K itself is large.
    for (let start = 0; start < heap.length; start += 1024) {
      signal?.throwIfAborted()
      const rows = heap.slice(start, start + 1024)
      yield {
        selection: { type: 'all', length: rows.length },
        columns: fields.map((field, i) => ({ type: 'values', values: rows.map(row => row.values[i]), length: rows.length })),
      }
    }
  }
}

/**
 * Original physical input order, independent of file visit order.
 * @param {TopKRow} a
 * @param {TopKRow} b
 * @returns {number}
 */
function comparePosition(a, b) {
  return a.entryIndex - b.entryIndex || a.batchIndex - b.batchIndex || a.rowIndex - b.rowIndex
}

/**
 * Only complete, non-null statistics can certify or exclude a file.
 * @param {ManifestEntry} entry
 * @param {number} id
 * @param {IcebergType} type
 * @returns {{lower: number | bigint | string, upper: number | bigint | string} | undefined}
 */
function fileBounds(entry, id, type) {
  const file = entry.data_file
  if (file.record_count <= 0n || metric(file.null_value_counts, id) !== 0n ||
      metric(file.value_counts, id) !== file.record_count) return undefined
  const lower = decode(metric(file.lower_bounds, id), type)
  const upper = decode(metric(file.upper_bounds, id), type)
  if (lower === undefined || upper === undefined || lower > upper) return undefined
  return { lower, upper }
}

/**
 * Iceberg's Avro int-keyed maps decode to key/value arrays.
 *
 * @param {any} map
 * @param {number} id
 * @returns {any}
 */
function metric(map, id) {
  return Array.isArray(map) ? map.find(e => Number(e.key) === id)?.value : map?.[id]
}

/**
 * Match SQL's millisecond Date resolution. Only ASCII string endpoints are
 * used: UTF-8 Iceberg bounds and SQL's UTF-16 comparison agree against these
 * endpoints, including truncated ISO date/time bounds.
 *
 * @param {Uint8Array | undefined} bytes
 * @param {IcebergType} type
 * @returns {number | bigint | string | undefined}
 */
function decode(bytes, type) {
  if (!bytes || typeof type !== 'string') return undefined
  const value = deserializeValue(bytes, type)
  if (type === 'string') {
    return typeof value === 'string' && Array.from(value).every(c => c.charCodeAt(0) < 128) ? value : undefined
  }
  if (type === 'date') {
    const millis = new Date(value * 86400000).getTime()
    return Number.isFinite(millis) ? millis : undefined
  }
  if (type.startsWith('timestamp')) {
    if (typeof value !== 'bigint') return undefined
    const millis = new Date(Number(value / (type.endsWith('_ns') ? 1000000n : 1000n))).getTime()
    return Number.isFinite(millis) ? millis : undefined
  }
  return typeof value === 'bigint' || typeof value === 'number' && Number.isFinite(value) ? value : undefined
}
