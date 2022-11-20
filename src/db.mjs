import { homedir } from 'os'
import { join } from 'path'

import SQLite from 'better-sqlite3'

const DB_VERSION = 5

const db = {}

let opened = false

function open () {
  if (opened) return
  opened = true

  const dbFile = process.env.DB || join(homedir(), '.databases', 'files.sqlite')
  const _db = new SQLite(dbFile)
  _db.pragma('foreign_keys=ON')
  if (getVersion(_db) !== DB_VERSION) {
    throw new Error('Wrong version of db: ' + dbFile)
  }

  Object.assign(db, buildStoredProcs(_db))
  Object.assign(db, buildQueries(_db))
}

db.open = open

export default db

function getVersion (db) {
  const sql = 'select version from dbversion'
  let p = db.prepare(sql)
  p = p.pluck()
  return p.get()
}

function buildStoredProcs (db) {
  const insertLocalFile = makeStoredProc(db, 'sp_insertLocalFile')
  const deleteLocalFile = makeStoredProc(db, 'sp_deleteLocalFile')
  const insertS3File = makeStoredProc(db, 'sp_insertS3File')
  const deleteS3File = makeStoredProc(db, 'sp_deleteS3File')
  const insertGdriveFile = makeStoredProc(db, 'sp_insertGdriveFile')
  const deleteGdriveFile = makeStoredProc(db, 'sp_deleteGdriveFile')
  return {
    insertLocalFiles: db.transaction(files => files.forEach(insertLocalFile)),
    insertS3Files: db.transaction(files => files.forEach(insertS3File)),
    insertGdriveFiles: db.transaction(files => files.forEach(insertGdriveFile)),
    deleteLocalFiles: db.transaction(files => files.forEach(deleteLocalFile)),
    deleteS3Files: db.transaction(files => files.forEach(deleteS3File)),
    deleteGdriveFiles: db.transaction(files => files.forEach(deleteGdriveFile))
  }
}

function buildQueries (db) {
  const ret = {}
  for (const [name, entry] of Object.entries(QUERIES)) {
    const [sql, params] = entry
    const stmt = db.prepare(sql)
    if (params.length) {
      ret[name] = x => stmt.all(chk(x, ...params))
    } else {
      ret[name] = () => stmt.all()
    }
  }

  // adjust exceptions
  {
    const fn = ret.locateLocalFile
    ret.locateLocalFile = x => fn(x)[0]
  }

  {
    const fn = ret.getDuplicates
    ret.getDuplicates = () => fn().map(x => x.contentId)
  }
  return ret
}

function makeStoredProc (db, name, noParams = false) {
  if (noParams) {
    const sql = `insert into ${name} values(null)`
    const stmt = db.prepare(sql)
    return () => stmt.run()
  } else {
    const params = getProcParams(db, name)
    const sql =
      `insert into ${name}(${params.join(',')}) ` +
      `values(${params.map(n => ':' + n).join(',')})`
    const stmt = db.prepare(sql)
    return data => stmt.run(chk(data, ...params))
  }
}

function getProcParams (db, name) {
  const sql = `select * from ${name}`
  return db
    .prepare(sql)
    .columns()
    .map(col => col.name)
}

function chk (data, ...params) {
  const ret = {}
  const supplied = Object.getOwnPropertyNames(data)
  for (const param of params) {
    if (!supplied.includes(param)) {
      console.error('Was expecting to see "%s" in "%o"', param, data)
      throw new Error('Bad parameter given')
    }
    let v = data[param]
    if (v instanceof Date) v = v.toISOString()
    ret[param] = v
  }
  return ret
}

const QUERIES = {
  getLocalFiles: [
    "select * from qry_localFiles where path glob :path || '*'",
    ['path']
  ],
  getS3Files: [
    'select * from qry_s3Files ' +
      "where bucket = :bucket and path glob :path || '*'",
    ['bucket', 'path']
  ],
  getGdriveFiles: [
    "select * from qry_gdriveFiles where path glob :path || '*'",
    ['path']
  ],
  locateLocalFile: [
    'select * from qry_localFiles ' +
      'where path = :path and size = :size and mtime = datetime(:mtime)',
    ['path', 'size', 'mtime']
  ],
  getLocalNotS3: [
    'select * from qry_localFiles ' +
      "where path glob :localPath || '*' " +
      'and contentId not in (' +
      'select contentId from qry_s3Files ' +
      "where bucket = :s3Bucket and path glob :s3Path || '*')",
    ['localPath', 's3Bucket', 's3Path']
  ],
  getS3NotLocal: [
    'select * from qry_s3Files ' +
      "where bucket = :s3Bucket and path glob :s3Path || '*' " +
      'and contentId not in (' +
      'select contentId from qry_localFiles ' +
      "where path glob :localPath || '*')",
    ['localPath', 's3Bucket', 's3Path']
  ],
  getLocalNotGdrive: [
    'select * from qry_localFiles ' +
      "where path glob :localPath || '*' " +
      'and contentId not in (' +
      'select contentId from qry_gdriveFiles ' +
      "where path glob :gdrivePath || '*')",
    ['localPath', 'gdrivePath']
  ],
  getGdriveNotLocal: [
    'select * from qry_gdriveFiles ' +
      "where path glob :gdrivePath || '*' " +
      'and contentId not in (' +
      'select contentId from qry_localFiles ' +
      "where path glob :localPath || '*')",
    ['localPath', 'gdrivePath']
  ],
  getLocalS3Diffs: [
    'select * from qry_localAndS3 ' +
      "where localPath glob :localPath || '*' " +
      'and s3Bucket = :s3Bucket ' +
      "and s3Path glob :s3Path || '*' " +
      'and substr(localPath, 1+length(:localPath)) != ' +
      'substr(s3Path, 1 + length(:s3Path))',
    ['localPath', 's3Bucket', 's3Path']
  ],
  getLocalGdriveDiffs: [
    'select * from qry_localAndGdrive ' +
      "where localPath glob :localPath || '*' " +
      "and gdrivePath glob :gdrivePath || '*' " +
      'and substr(localPath, 1+length(:localPath)) != ' +
      'substr(gdrivePath, 1 + length(:gdrivePath))',
    ['localPath', 'gdrivePath']
  ],
  getLocalContent: [
    'select * from qry_localFiles ' +
      'where contentId = :contentId ' +
      "and path glob :localPath || '*'",
    ['contentId', 'localPath']
  ],
  getS3Content: [
    'select * from qry_s3Files ' +
      'where contentId = :contentId ' +
      'and bucket = :s3Bucket ' +
      "and path glob :s3Path || '*'",
    ['contentId', 's3Bucket', 's3Path']
  ],
  getGdriveContent: [
    'select * from qry_gdriveFiles ' +
      'where contentId = :contentId ' +
      "and path glob :gdrivePath || '*'",
    ['contentId', 'gdrivePath']
  ],
  getDuplicates: ['select * from qry_duplicates', []]
}
