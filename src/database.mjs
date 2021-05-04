import JSDB from 'jsdb'
import sortBy from 'sortby'
import once from 'pixutil/once'

import { urlrelative, urljoin, urldirname, urlbasename } from './util.mjs'

class Database {
  constructor () {
    this.dbPath = new JSDB('url_path.db')
    this.dbHash = new JSDB('url_hash.db')
  }

  async prepare () {
    await this.dbPath.ensureIndex({ fieldName: 'url', unique: true })
    await this.dbHash.ensureIndex({ fieldName: 'dir' })
  }

  async * rows (prefix, filter) {
    for await (const row of this._rows(prefix)) {
      const path = urlrelative(prefix, row.url)
      if (!filter(path)) continue
      yield { ...row, path }
    }
  }

  async * _rows (prefix) {
    const paths = (await this.dbPath.getAll())
      .filter(({ url }) => url.startsWith(prefix))
      .sort(sortBy('url'))
    for (const path of paths) {
      const files = (await this.dbHash.find('dir', path._id)).sort(
        sortBy('file')
      )
      for (const row of files) {
        const { _id, dir, file, ...rest } = row
        const url = urljoin(path.url, file)
        yield { url, ...rest }
      }
    }
  }

  async store (data) {
    const { _id, url, ...rest } = data
    const path = urldirname(url)
    const file = urlbasename(url)
    let dir = await this.dbPath.findOne('url', path)
    if (!dir) dir = await this.dbPath.insert({ url: path })

    const files = await this.dbHash.find('dir', dir._id)
    const row = files.find(r => r.file === file)
    if (row) {
      await this.dbHash.update({ ...row, ...rest })
    } else {
      await this.dbHash.insert({ dir: dir._id, file, ...rest })
    }
  }

  async remove ({ url }) {
    const path = urldirname(url)
    const file = urlbasename(url)
    const dir = await this.dbPath.findOne('url', path)
    if (!dir) return

    const files = await this.dbHash.find('dir', dir._id)
    const row = files.find(r => r.file === file)
    if (row) {
      await this.dbHash.delete(row)
      if (files.length === 1) {
        await this.dbPath.delete(dir)
      }
    }
  }

  async compact () {
    await this.dbPath.compact({ sortBy: sortBy('url') })
    await this.dbHash.compact({ sortBy: sortBy('dir').thenBy('file') })
  }
}

export const getDB = once(async function getDB () {
  const db = new Database()
  await db.prepare()
  return db
})
