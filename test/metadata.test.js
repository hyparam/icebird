import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { avroMetadata } from '../src/avro.metadata.js'
import { asyncBufferFromFile, toJson } from 'hyparquet'
import { fileToJson } from './helpers.js'

describe('avroMetadata from test files', () => {
  const files = fs.readdirSync('test/files').filter(f => f.endsWith('.avro'))

  files.forEach(filename => {
    it(`parse metadata from ${filename}`, async () => {
      const file = await asyncBufferFromFile(`test/files/${filename}`)
      const buffer = await file.slice(0)
      const reader = { view: new DataView(buffer), offset: 0 }
      const metadata = await avroMetadata(reader)

      const base = filename.replace('.avro', '')
      const expected = fileToJson(`test/files/${base}.metadata.json`)
      // stringify and parse to make legal json
      expect(JSON.parse(JSON.stringify(toJson(metadata)))).toEqual(expected)
    })
  })
})
