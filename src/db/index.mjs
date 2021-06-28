import { homedir } from 'os'
import { resolve } from 'path'

import SQLite from 'better-sqlite3'

import SQL from '../lib/sql.mjs'
import * as sql from './sql.mjs'

const DBVERSION = 3

let opened
export function open () {
  if (opened) return
  opened = true
  const dbFile =
    process.env.DB || resolve(homedir(), '.databases', 'files2.sqlite')
  const db = new SQLite(dbFile)
  SQL.attach(db)
  sql.ddl()
  const version = db
    .prepare('select version from dbversion')
    .pluck()
    .get()
  if (version !== DBVERSION) {
    throw new Error('Wrong version of database: ' + dbFile)
  }
}

export const insertSyncFiles = SQL.transaction((type, files) => {
  for (const { path, url, mtime: _mtime, size } of files) {
    const mtime = _mtime.toISOString()
    sql.insertSync({ type, path, url, mtime, size })
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

export function clearSync () {
  return sql.clearSync()
}

export function selectHash ({ url, mtime: _mtime, size }) {
  const mtime = _mtime.toISOString()
  return sql.selectHash.pluck().get({ url, mtime, size })
}

export function insertHash ({ url, mtime: _mtime, size, hash }) {
  const mtime = _mtime.toISOString()
  sql.insertHash({ url, mtime, size, hash })
}

export function deleteHash ({ url }) {
  sql.deleteHash({ url })
}
