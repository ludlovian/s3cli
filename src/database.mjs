import Database from 'jsdb'

import { once } from './util.mjs'

export const getDB = once(async function getDB () {
  const db = new Database('url_md5_cache.db')
  return db
})

export async function removeRow (row) {
  const db = await getDB()
  await db.delete(row)
}

export async function compactDatabase () {
  const db = await getDB()
  await db.compact('url')
}
