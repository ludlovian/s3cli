import EventEmitter from 'events'
import { relative } from 'path'

import { parseAddress as s3parse, scan as s3scan, stat as s3stat } from 's3js'

import { getDB } from './database.mjs'
import { sortBy } from './util.mjs'

export default class Remote extends EventEmitter {
  constructor (data) {
    super()
    Object.assign(this, data)
  }

  static async * files (root, filter) {
    const { Bucket, Key: Prefix } = s3parse(root)
    for await (const data of s3scan(root + '/')) {
      const path = relative(Prefix, data.Key)
      if (data.Key.endsWith('/') || !filter(path)) continue
      yield new Remote({
        path,
        root,
        url: `s3://${Bucket}/${data.Key}`,
        mtime: +data.LastModified,
        size: data.Size
      })
    }
  }

  static async * hashes (root, filter) {
    const { Bucket, Key: Prefix } = s3parse(root)
    const db = await getDB()
    const rows = (await db.getAll())
      .filter(({ url }) => url.startsWith(`s3://${Bucket}/${Prefix}`))
      .sort(sortBy('url'))
    for (const row of rows) {
      const url = new URL(row.url)
      const path = relative(Prefix, url.pathname.replace(/^\//, ''))
      if (!filter(path)) continue
      yield { ...row, path }
    }
  }

  async getHash (row) {
    if (row && row.mtime === this.mtime && row.size === this.size) {
      this.hash = row.hash
      return
    }

    this.emit('hashing')
    const stats = await s3stat(this.url)
    this.hash = stats.md5 || 'UNKNOWN'

    const db = await getDB()
    await db.upsert({
      ...(row || {}),
      url: this.url,
      mtime: this.mtime,
      size: this.size,
      hash: this.hash,
      path: undefined
    })
  }
}
