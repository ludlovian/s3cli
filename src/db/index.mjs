import { homedir } from 'os'
import { resolve } from 'path'

import SQLite from 'better-sqlite3'

import SQL from '../lib/sql.mjs'
import * as sql from './sql.mjs'

const DBVERSION = 4

let opened
export function open () {
  if (opened) return
  opened = true
  const dbFile =
    process.env.DB || resolve(homedir(), '.databases', 'files4.sqlite')
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

export const insertS3Files = SQL.transaction(files =>
  files.forEach(file => sql.insertS3File(file))
)

export const insertLocalFiles = SQL.transaction(files =>
  files.forEach(file => sql.insertLocalFile(file))
)
