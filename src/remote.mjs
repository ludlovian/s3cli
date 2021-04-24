import EventEmitter from 'events'
import { relative } from 'path'

import Database from 'jsdbd'
import { parseAddress as s3parse, scan as s3scan, stat as s3stat } from 's3js'

import { once } from './util.mjs'

export default class Remote extends EventEmitter {
  constructor (data) {
    super()
    Object.assign(this, data)
    if (this.etag && !this.etag.includes('-')) {
      this.hash = this.etag
    }
  }

  static async * scan (root, filter) {
    const { Bucket, Key: Prefix } = s3parse(root)
    for await (const data of s3scan(root + '/')) {
      const path = relative(Prefix, data.Key)
      if (data.Key.endsWith('/') || !filter(path)) continue
      yield new Remote({
        path,
        root,
        url: `${Bucket}/${data.Key}`,
        etag: data.ETag.replace(/"/g, '')
      })
    }
  }

  async getHash () {
    if (this.hash) return this.hash
    const db = await getDB()
    const rec = await db.findOne('url', this.url)
    if (rec) {
      if (this.etag === rec.etag) {
        this.hash = rec.hash
        return this.hash
      }
    }

    this.emit('hashing')
    const stats = await s3stat(`s3://${this.url}`)

    this.hash = stats.md5 || 'UNKNOWN'
    await db.upsert({
      ...(rec || {}),
      url: this.url,
      etag: this.etag,
      hash: this.hash
    })

    return this.hash
  }
}

const getDB = once(async () => {
  const db = new Database('s3file_md5_cache.db')
  await db.check()
  await db.ensureIndex({ fieldName: 'url', unique: true })
  return db
})
