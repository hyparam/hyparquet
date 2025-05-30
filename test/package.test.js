import { describe, expect, it } from 'vitest'
import packageJson from '../package.json' with { type: 'json' }

describe('package.json', () => {
  it('should have the correct name', () => {
    expect(packageJson.name).toBe('hyparquet')
  })
  it('should have a valid version', () => {
    expect(packageJson.version).toMatch(/^\d+\.\d+\.\d+$/)
  })
  it('should have MIT license', () => {
    expect(packageJson.license).toBe('MIT')
  })
  it('should have precise dev dependency versions', () => {
    const { devDependencies } = packageJson
    Object.values(devDependencies).forEach(version => {
      expect(version).toMatch(/^\d+\.\d+\.\d+$/)
    })
  })
  it('should have no dependencies', () => {
    expect('dependencies' in packageJson).toBe(false)
    expect('peerDependencies' in packageJson).toBe(false)
  })
  it('should have exports with types first', () => {
    const { exports } = packageJson
    expect(Object.keys(exports)).toEqual(['.', './src/*.js'])
    // node vs default (browser)
    expect(Object.keys(exports['.'])).toEqual(['browser', 'default'])
    expect(Object.keys(exports['.'].browser)).toEqual(['types', 'import'])
    expect(Object.keys(exports['.'].default)).toEqual(['types', 'import'])
    // deep imports
    expect(Object.keys(exports['./src/*.js'])).toEqual(['types', 'import'])
  })
})
