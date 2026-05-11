import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../src/catalog/file.js'
import { icebergCreate } from '../src/create.js'
import { icebergCreateTable } from '../src/write/write.js'
import { memResolver } from './helpers.js'

/**
 * @import {Schema} from '../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
  ],
}

describe('icebergCreate({ conditionalCommits })', () => {
  it('writes v1.metadata.json with ifNoneMatch when enabled', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-create-flag'
    const { resolver, files } = memResolver()

    /** @type {{ path: string, ifNoneMatch?: string }[]} */
    const calls = []
    const writerSpy = resolver.writer
    if (!writerSpy) throw new Error('writer required')
    resolver.writer = (p, options) => {
      calls.push({ path: p, ifNoneMatch: options?.ifNoneMatch })
      return writerSpy(p, options)
    }

    await icebergCreate({ tableUrl, resolver, schema, conditionalCommits: true })

    const metaCall = calls.find(c => c.path.endsWith('/metadata/v1.metadata.json'))
    const hintCall = calls.find(c => c.path.endsWith('/metadata/version-hint.text'))
    expect(metaCall?.ifNoneMatch).toBe('*')
    expect(hintCall?.ifNoneMatch).toBeUndefined()
    expect(files.has(`${tableUrl}/metadata/v1.metadata.json`)).toBe(true)
    expect(files.has(`${tableUrl}/metadata/version-hint.text`)).toBe(true)
  })

  it('writes without ifNoneMatch by default (backwards compatible)', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-create-default'
    const { resolver } = memResolver()

    /** @type {{ path: string, ifNoneMatch?: string }[]} */
    const calls = []
    const writerSpy = resolver.writer
    if (!writerSpy) throw new Error('writer required')
    resolver.writer = (p, options) => {
      calls.push({ path: p, ifNoneMatch: options?.ifNoneMatch })
      return writerSpy(p, options)
    }

    await icebergCreate({ tableUrl, resolver, schema })

    expect(calls.every(c => c.ifNoneMatch === undefined)).toBe(true)
  })

  it('rejects with status 412 when v1 already exists', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-create-collision'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })
    await expect(icebergCreateTable({ catalog, tableUrl, schema }))
      .rejects.toMatchObject({ status: 412 })
  })

  it('overwrites by default when v1 already exists', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-create-overwrite'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreateTable({ catalog, tableUrl, schema })
    // No flag: old behavior is to silently clobber. Verifies we did not
    // change the default semantics.
    await expect(icebergCreateTable({ catalog, tableUrl, schema })).resolves.toBeDefined()
  })

  it('concurrent creates: exactly one wins under conditionalCommits', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-create-race'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    const results = await Promise.allSettled([
      icebergCreateTable({ catalog, tableUrl, schema }),
      icebergCreateTable({ catalog, tableUrl, schema }),
      icebergCreateTable({ catalog, tableUrl, schema }),
    ])

    const ok = results.filter(r => r.status === 'fulfilled')
    const failed = results.filter(r => r.status === 'rejected')
    expect(ok).toHaveLength(1)
    expect(failed).toHaveLength(2)
    for (const f of failed) {
      const { reason } = /** @type {PromiseRejectedResult} */ (f)
      expect(reason).toMatchObject({ status: 412 })
    }
    expect(files.has(`${tableUrl}/metadata/v1.metadata.json`)).toBe(true)
  })

  it('hint failure does not fail the commit when conditionalCommits is on', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-create-hint-fail'
    const { resolver, files } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    resolver.writer = (p, options) => {
      const w = realWriter(p, options)
      if (p.endsWith('/metadata/version-hint.text')) {
        w.finish = () => Promise.reject(new Error('hint blocked: 503'))
      }
      return w
    }
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    const metadata = await icebergCreateTable({ catalog, tableUrl, schema })

    expect(metadata['format-version']).toBe(2)
    expect(files.has(`${tableUrl}/metadata/v1.metadata.json`)).toBe(true)
    expect(files.has(`${tableUrl}/metadata/version-hint.text`)).toBe(false)
  })

  it('hint failure still propagates without conditionalCommits', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-create-hint-fail-default'
    const { resolver } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    resolver.writer = (p, options) => {
      const w = realWriter(p, options)
      if (p.endsWith('/metadata/version-hint.text')) {
        w.finish = () => Promise.reject(new Error('hint blocked: 503'))
      }
      return w
    }
    const catalog = fileCatalog({ resolver })

    await expect(icebergCreateTable({ catalog, tableUrl, schema }))
      .rejects.toThrow(/hint blocked/)
  })
})
