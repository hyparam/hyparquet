import { describe, expect, it } from 'vitest'
import packageJson from '../package.json'

describe('package.json', () => {
  it('should have the correct name', () => {
    expect(packageJson.name).toBe('hyparquet')
  })
  it('should have a valid version', () => {
    expect(packageJson.version).toMatch(/^\d+\.\d+\.\d+$/)
  })
  it('should have precise dependency versions', () => {
    const { devDependencies } = packageJson
    Object.values(devDependencies).forEach(version => {
      expect(version).toMatch(/^\d+\.\d+\.\d+$/)
    })
  })
})
