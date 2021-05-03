import EventEmitter from 'events'
import { relative, join } from 'path'

import filescan from 'filescan'
import hashFile from 'hash-stream/simple'

import { getDB } from './database.mjs'

export default class Local extends EventEmitter {
  constructor (data) {
    super()
    Object.assign(this, data)
  }

  static async * files (root, filter) {
    for await (const { path: fullpath, stats } of filescan(root)) {
      if (!stats.isFile()) continue
      const path = relative(root, fullpath)
      if (!filter(path)) continue
      yield new Local({ path, fullpath, root, stats })
    }
  }

  static async * hashes (root, filter) {
    const db = await getDB()
    yield * db.rows('file://' + root, filter)
  }

  async getHash (row) {
    const stats = this.stats
    if (row && stats.mtimeMs === row.mtime && stats.size === row.size) {
      this.hash = row.hash
      return
    }

    this.emit('hashing')
    this.hash = await hashFile(this.fullpath)

    const db = await getDB()
    await db.store({
      url: `file://${join(this.root, this.path)}`,
      mtime: this.stats.mtimeMs,
      size: this.stats.size,
      hash: this.hash
    })
  }
}
