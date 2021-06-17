import { homedir } from 'os'
import { resolve } from 'path'

import SQLite from 'better-sqlite3'

import { ddl, sql } from './sql.mjs'

const DB_DIR = process.env.DB_DIR || resolve(homedir(), '.databases')
const DB_FILE = process.env.DB_FILE || 'files2.sqlite'

const db = new SQLite(resolve(DB_DIR, DB_FILE))
db.pragma('journal_mode=WAL')
db.exec(ddl)

for (const k in sql) sql[k] = db.prepare(sql[k])

export const insertHashes = db.transaction(hashes => {
  for (const { url, mtime: _mtime, size, hash } of hashes) {
    const mtime = _mtime.toISOString()
    sql.insertHash.run({ url, mtime, size, hash })
  }
})

export const insertSyncFiles = db.transaction((type, files) => {
  for (const { path, url, mtime: _mtime, size } of files) {
    const mtime = _mtime.toISOString()
    sql.insertSync.run({ type, path, url, mtime, size })
  }
})

export function selectMissingFiles () {
  return sql.selectMissingFiles.all()
}

export function selectMissingHashes () {
  return sql.selectMissingHashes.pluck().all()
}

export function selectChanged () {
  return sql.selectChanged.all()
}

export function selectSurplusFiles () {
  return sql.selectSurplusFiles.pluck().all()
}

export function countFiles () {
  return sql.countFiles.pluck().get()
}

export function selectHash ({ url, mtime: _mtime, size }) {
  const mtime = _mtime.toISOString()
  return sql.selectHash.pluck().get({ url, mtime, size })
}

export function insertHash ({ url, mtime: _mtime, size, hash }) {
  const mtime = _mtime.toISOString()
  sql.insertHash.run({ url, mtime, size, hash })
}
