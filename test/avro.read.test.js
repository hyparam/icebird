import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { avroRead } from '../src/avro/avro.read.js'
import { avroMetadata } from '../src/avro/avro.metadata.js'
import { asyncBufferFromFile, toJson } from 'hyparquet'
import { fileToJson } from './helpers.js'

describe('avroRead from test files', () => {
  const files = fs.readdirSync('test/files').filter(f => f.endsWith('.avro'))

  files.forEach(filename => {
    it(`parse metadata from ${filename}`, async () => {
      const file = await asyncBufferFromFile(`test/files/${filename}`)
      const buffer = await file.slice(0)
      const reader = { view: new DataView(buffer), offset: 0 }
      const { metadata, syncMarker } = await avroMetadata(reader)
      const data = await avroRead({ reader, metadata, syncMarker })

      const base = filename.replace('.avro', '')
      const expected = fileToJson(`test/files/${base}.json`)
      // convert to legal json
      expect(toJson(data)).toEqual(expected)
    })
  })
})
