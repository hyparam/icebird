import avro from 'avsc'
import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { toJson } from 'hyparquet'
import { fileToJson } from './helpers.js'
import { parseHook } from '../src/avro.js'

describe('avro data from avsc', () => {
  const files = fs.readdirSync('test/files').filter(f => f.endsWith('.avro'))

  files.forEach(filename => {
    it(`parse metadata from ${filename}`, async () => {
      /** @type {any[]} */
      const records = []
      const data = await new Promise((resolve, reject) => {
        const decoder = avro.createFileDecoder(`test/files/${filename}`, {
          parseHook,
        })
        decoder.on('data', record => {
          // console.log('record', record)
          records.push(record)
        })
        decoder.on('end', () => resolve(records))
        decoder.on('error', reject)
      })

      const base = filename.replace('.avro', '')
      const expected = fileToJson(`test/files/${base}.json`)
      // convert to legal json
      expect(toJson(data)).toEqual(expected)
    })
  })
})
