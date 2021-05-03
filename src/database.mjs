import { relative } from 'path'

import JSDB from 'jsdb'
import sortBy from 'sortby'

import { once } from './util.mjs'

class Database {
  constructor () {
    this.db = new JSDB('url_md5_cache.db')
  }

  async prepare () {
    await this.db.ensureIndex({ fieldName: 'url', unique: true })
  }

  async * rows (prefix, filter) {
    const urlPrefix = new URL(prefix)
    const rows = (await this.db.getAll())
      .filter(({ url }) => url.startsWith(urlPrefix.href))
      .sort(sortBy('url'))
    for (const row of rows) {
      const urlRow = new URL(row.url)
      const path = relative(urlPrefix.pathname, urlRow.pathname)
      if (!filter(path)) continue
      yield { ...row, path }
    }
  }

  async store (data) {
    const { _id, url, ...rest } = data
    const row = await this.db.findOne('url', url)
    if (row) {
      await this.db.update({ ...row, ...rest })
    } else {
      await this.db.insert({ url, ...rest })
    }
  }

  async remove ({ url }) {
    const row = await this.db.findOne('url', url)
    if (row) await this.db.delete(row)
  }

  async compact () {
    await this.db.compact({ sorted: 'url' })
  }
}

export const getDB = once(async function getDB () {
  const db = new Database()
  await db.prepare()
  return db
})
