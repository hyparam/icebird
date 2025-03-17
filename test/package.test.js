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
  it('should have precise dev dependency versions', () => {
    const { dependencies, devDependencies } = packageJson
    const allDependencies = { ...dependencies, ...devDependencies }
    Object.values(allDependencies).forEach(version => {
      expect(version).toMatch(/^\d+\.\d+\.\d+$/)
    })
  })
  it('should have no peer dependencies', () => {
    expect('peerDependencies' in packageJson).toBe(false)
  })
})
