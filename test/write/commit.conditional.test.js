import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { icebergCreate } from '../../src/create.js'
import { icebergAppend, icebergCreateTable } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Schema} from '../../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: false, type: 'string' },
  ],
}

describe('fileCatalogCommit conditional create', () => {
  it('passes ifNoneMatch on the new metadata file when enabled', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-flag'
    const { resolver } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')

    /** @type {{ path: string, ifNoneMatch?: string }[]} */
    const calls = []
    resolver.writer = (p, options) => {
      calls.push({ path: p, ifNoneMatch: options?.ifNoneMatch })
      return realWriter(p, options)
    }
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    await icebergCreate({ tableUrl, resolver, schema })
    calls.length = 0
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })

    const v2 = calls.find(c => c.path.endsWith('/metadata/v2.metadata.json'))
    const hint = calls.find(c => c.path.endsWith('/metadata/version-hint.text'))
    expect(v2?.ifNoneMatch).toBe('*')
    expect(hint?.ifNoneMatch).toBeUndefined()
  })

  it('writes without ifNoneMatch by default', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-default'
    const { resolver } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')

    /** @type {{ path: string, ifNoneMatch?: string }[]} */
    const calls = []
    resolver.writer = (p, options) => {
      calls.push({ path: p, ifNoneMatch: options?.ifNoneMatch })
      return realWriter(p, options)
    }
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    calls.length = 0
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })

    expect(calls.every(c => c.ifNoneMatch === undefined)).toBe(true)
  })

  it('rejects with status 412 when v2 already exists', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-collision'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })
    // Plant a foreign v2.metadata.json before our writer reaches finish().
    files.set(`${tableUrl}/metadata/v2.metadata.json`, new TextEncoder().encode('{}'))

    await expect(icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
    })).rejects.toMatchObject({ status: 412 })
  })

  it('overwrites v2 by default (backwards compatible)', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-overwrite-default'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreateTable({ catalog, tableUrl, schema })
    files.set(`${tableUrl}/metadata/v2.metadata.json`, new TextEncoder().encode('{}'))

    const committed = await icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
    })
    expect(committed.snapshots).toHaveLength(1)
    // overwrite happened
    const v2 = files.get(`${tableUrl}/metadata/v2.metadata.json`)
    expect(v2 && v2.byteLength).toBeGreaterThan(2)
  })

  it('hint failure does not fail the commit when conditionalCommits is on', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-hint-fail'
    const { resolver, files } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    // First, create the table successfully (hint write must work for create
    // because we install the failure-injecting writer afterward).
    await icebergCreateTable({ catalog, tableUrl, schema })

    // Now make subsequent hint writes fail.
    resolver.writer = (p, options) => {
      const w = realWriter(p, options)
      if (p.endsWith('/metadata/version-hint.text')) {
        w.finish = async () => { throw new Error('hint blocked: 503') }
      }
      return w
    }

    const committed = await icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
    })
    expect(committed.snapshots).toHaveLength(1)
    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
    // Hint left at v1 (unchanged from create).
    const hint = files.get(`${tableUrl}/metadata/version-hint.text`)
    expect(hint && new TextDecoder().decode(hint)).toBe('1')
  })

  it('hint failure still propagates without conditionalCommits', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-hint-fail-default'
    const { resolver } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    const catalog = fileCatalog({ resolver })

    await icebergCreateTable({ catalog, tableUrl, schema })

    resolver.writer = (p, options) => {
      const w = realWriter(p, options)
      if (p.endsWith('/metadata/version-hint.text')) {
        w.finish = async () => { throw new Error('hint blocked: 503') }
      }
      return w
    }

    await expect(icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
    })).rejects.toThrow(/hint blocked/)
  })

  it('concurrent appends: only one wins under conditionalCommits', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-race'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })

    const results = await Promise.allSettled([
      icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] }),
      icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'b' }] }),
      icebergAppend({ catalog, tableUrl, records: [{ id: 3n, name: 'c' }] }),
    ])

    const ok = results.filter(r => r.status === 'fulfilled')
    const failed = results.filter(r => r.status === 'rejected')
    expect(ok).toHaveLength(1)
    expect(failed).toHaveLength(2)
    for (const f of failed) {
      const { reason } = /** @type {PromiseRejectedResult} */ (f)
      expect(reason).toMatchObject({ status: 412 })
    }
    // Exactly v2 was committed.
    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
    expect(files.has(`${tableUrl}/metadata/v3.metadata.json`)).toBe(false)
  })
})
