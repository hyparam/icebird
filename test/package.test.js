import { describe, expect, it } from 'vitest'
import packageJson from '../package.json' with { type: 'json' }

describe('package.json', () => {
  it('should have the correct name', () => {
    expect(packageJson.name).toBe('icebird')
  })

  it('should have a valid version', () => {
    expect(packageJson.version).toMatch(/^\d+\.\d+\.\d+$/)
  })

  it('should have MIT license', () => {
    expect(packageJson.license).toBe('MIT')
  })

  it('should have the expected dependencies', () => {
    const { dependencies } = packageJson
    expect(Object.keys(dependencies)).toEqual([
      'hyparquet',
      'hyparquet-compressors',
      'hyparquet-writer',
      'squirreling',
    ])
  })

  it('should have precise dependency versions', () => {
    const { dependencies, devDependencies } = packageJson
    const allDependencies = { ...dependencies, ...devDependencies }
    Object.values(allDependencies).forEach(version => {
      expect(version).toMatch(/^\d+\.\d+\.\d+$/)
    })
  })

  it('should declare the optional AWS peer dependency for icebird/s3tables', () => {
    const { peerDependencies, peerDependenciesMeta } = packageJson
    expect(peerDependencies).toEqual({
      '@aws-sdk/credential-providers': '^3.0.0',
    })
    expect(peerDependenciesMeta?.['@aws-sdk/credential-providers']?.optional).toBe(true)
  })

  it('should have exports with types first', () => {
    const { exports } = packageJson
    expect(exports).toBeDefined()
    for (const [, exportObj] of Object.entries(exports)) {
      if (typeof exportObj === 'object') {
        expect(Object.keys(exportObj)).toEqual(['types', 'import'])
      } else {
        expect(typeof exportObj).toBe('string')
      }
    }
  })
})
