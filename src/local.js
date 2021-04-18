'use strict'

import crypto from 'crypto'
import EventEmitter from 'events'
import fs from 'fs'
import { resolve, relative } from 'path'

import Database from 'jsdbd'
import filescan from 'filescan'

import { once } from './util'

export default class Local extends EventEmitter {
  constructor (data) {
    super()
    Object.assign(this, data)
  }

  static async * scan (root, filter) {
    root = resolve(root)
    for await (const { path: fullpath, stats } of filescan(root)) {
      if (!stats.isFile()) continue
      const path = relative(root, fullpath)
      if (!filter(path)) continue
      yield new Local({ path, fullpath, root, stats })
    }
  }

  async getHash () {
    if (this.hash) return this.hash
    const db = await getDB()
    this.fullpath = await fs.promises.realpath(this.fullpath)
    if (!this.stats) this.stats = await fs.promises.stat(this.fullpath)
    const rec = await db.findOne('path', this.fullpath)
    if (rec) {
      if (this.stats.mtimeMs === rec.mtime && this.stats.size === rec.size) {
        this.hash = rec.hash
        return this.hash
      }
    }

    this.emit('hashing')
    this.hash = await hashFile(this.fullpath)

    await db.upsert({
      ...(rec || {}),
      path: this.fullpath,
      mtime: this.stats.mtimeMs,
      size: this.stats.size,
      hash: this.hash
    })

    return this.hash
  }
}

const getDB = once(async () => {
  const db = new Database('file_md5_cache.db')
  await db.check()
  await db.ensureIndex({ fieldName: 'path', unique: true })
  return db
})

async function hashFile (file) {
  const rs = fs.createReadStream(file)
  const hasher = crypto.createHash('md5')
  for await (const chunk of rs) {
    hasher.update(chunk)
  }
  return hasher.digest('hex')
}
