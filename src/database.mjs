import { homedir } from 'os'
import { resolve } from 'path'

import SQLite3 from 'better-sqlite3'

const db = new SQLite3(resolve(homedir(), '.databases', 'files.sqlite'))

const selectLocal = db.prepare(
  "SELECT 'file://' || path AS url, mtime, size, hash " +
    'FROM local_files ORDER BY url ASC;'
)
const selectS3 = db.prepare(
  "SELECT 's3://' || bucket || '/' || path AS url, mtime, size, hash " +
    'FROM s3_files ORDER BY url ASC;'
)

function * rows (prefix, filter) {
  if (!prefix.endsWith('/')) prefix += '/'
  let select
  if (prefix.startsWith('file://')) select = selectLocal
  else if (prefix.startsWith('s3://')) select = selectS3
  else select = { all: () => [] }
  for (const row of select.all()) {
    if (!row.url.startsWith(prefix)) continue
    const path = row.url.slice(prefix.length)
    if (!filter(path)) continue
    yield { ...row, path }
  }
}

const replaceLocal = db.prepare(
  'REPLACE INTO local_files(path, mtime, size, hash) ' +
    'VALUES($path, $mtime, $size, $hash);'
)
const replaceS3 = db.prepare(
  'REPLACE INTO s3_files(bucket, path, mtime, size, hash) ' +
    'VALUES($bucket, $path, $mtime, $size, $hash);'
)

function store ({ url, mtime, size, hash }) {
  if (url.startsWith('file://')) {
    const path = url.slice(7)
    replaceLocal.run({ path, mtime, size, hash })
  } else if (url.startsWith('s3://')) {
    const u = new URL(url)
    const bucket = u.hostname
    const path = u.pathname.replace(/^\//, '')
    replaceS3.run({ bucket, path, mtime, size, hash })
  }
}

const deleteLocal = db.prepare('DELETE FROM local_files WHERE path = $path;')
const deleteS3 = db.prepare(
  'DELETE FROM s3_files WHERE bucket = $bucket AND path = $path;'
)
function remove ({ url }) {
  if (url.startsWith('file://')) {
    const path = url.slice(7)
    deleteLocal.run({ path })
  } else if (url.startsWith('s3://')) {
    const u = new URL(url)
    const bucket = u.hostname
    const path = u.pathname.replace(/^\//, '')
    deleteS3.run({ bucket, path })
  }
}

function compact () {}

export async function getDB () {
  return { rows, store, remove, compact }
}
