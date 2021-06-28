import { test } from 'uvu'
import * as assert from 'uvu/assert'

import { readFileSync } from 'fs'
import { execFileSync } from 'child_process'

import SQLite from 'better-sqlite3'

const DBFILE = 'test/assets/test.sqlite'
let db

// test data used
//  srcRoot: "src/"
//  dstRoot: "dest/"
//  files:
//
//  file1 - matches on stats and hash
//  file2 - missing src hash which will be the same
//  file3 - dst hash for wrong stats, which will be different
//  file4 - exists on src, not dst
//  file5 - exists on dst, not src

test.before(() => {
  execFileSync('rm', ['-rf', 'test/assets'])
  execFileSync('mkdir', ['-p', 'test/assets'])
  db = new SQLite(DBFILE)
  run('ddl', { exec: true })
})

test.after(() => {
  execFileSync('rm', ['-rf', 'test/assets'])
})

test('create a new sync', () => {
  run('insertSync', {
    run: {
      type: 'src',
      path: 'oldpath',
      url: 'oldurl',
      mtime: 'oldmtime',
      size: 'oldsize'
    }
  })

  run('clearSync', { run: [] })

  let res
  res = runSQL('select count(*) from sync', { pluck: true, get: [] })
  assert.equal(res, 0)

  db.exec('BEGIN transaction')
  ;[
    ['src/file1', 'src1', 111, 'hash1'],
    ['src/file3', 'src3', 333, 'hash3'],
    ['src/file4', 'src4', 444, 'hash4'],
    ['dest/file1', 'dst1', 1111, 'hash1'],
    ['dest/file2', 'dst2', 2222, 'hash2'],
    ['dest/file3', 'XXX3', 9999, 'oldX3'],
    ['dest/file5', 'dst5', 5555, 'hash5']
  ].forEach(([url, mtime, size, hash]) =>
    run('insertHash', { run: { url, mtime, size, hash } })
  )
  ;[
    ['src', 'file1', 'src/file1', 'src1', 111],
    ['src', 'file2', 'src/file2', 'src2', 222],
    ['src', 'file3', 'src/file3', 'src3', 333],
    ['src', 'file4', 'src/file4', 'src4', 444],
    ['dst', 'file1', 'dest/file1', 'dst1', 1111],
    ['dst', 'file2', 'dest/file2', 'dst2', 2222],
    ['dst', 'file3', 'dest/file3', 'dst3', 3333],
    ['dst', 'file5', 'dest/file5', 'dst5', 5555]
  ].forEach(([type, path, url, mtime, size]) =>
    run('insertSync', { run: { type, path, url, mtime, size } })
  )
  db.exec('COMMIT;')
})

test('missing hash', () => {
  let urls = run('selectMissingHashes', { pluck: true, all: {} })
  const exp = ['dest/file3', 'src/file2']
  assert.equal(urls, exp)
  run('insertHash', {
    run: { url: 'src/file2', mtime: 'src2', size: 222, hash: 'hash2' }
  })
  run('insertHash', {
    run: { url: 'dest/file3', mtime: 'dst3', size: 3333, hash: 'oldH3' }
  })

  urls = run('selectMissingHashes', { pluck: true, all: {} })
  assert.equal(urls, [])
})

test('replace', () => {
  let res = run('selectChanged', { all: [] })
  const exp = [{ from: 'src/file3', to: 'dest/file3' }]
  assert.equal(res, exp)

  const type = 'dst'
  const size = 3333
  const mtime = 'dst3a'
  const hash = 'hash3'
  const url = 'dest/file3'
  run('insertHash', { run: { url, mtime, size, hash } })
  run('updateCopiedSync', { run: { url } })

  res = run('selectChanged', { all: [] })
  assert.equal(res, [])
})

test('copy', () => {
  let res = run('selectMissingFiles', { all: [] })
  const exp = [{ url: 'src/file4', path: 'file4' }]
  assert.equal(res, exp)

  const type = 'dst'
  const size = 4444
  const mtime = 'dst4'
  const hash = 'hash4'
  const url = 'dest/file4'
  run('insertHash', { run: { url, mtime, size, hash } })
  run('insertSync', { run: { type, path: 'file4', url, mtime, size } })

  res = run('selectMissingFiles', { all: [] })
  assert.equal(res, [])
})

test('delete', () => {
  let res = run('selectSurplusFiles', { pluck: true, all: [] })
  const exp = ['dest/file5']
  assert.equal(res, exp)

  const [url] = res
  run('deleteHash', { run: { url } })
  runSQL('delete from sync where url=?', { run: [url] })

  res = run('selectSurplusFiles', { pluck: true, all: [] })
  assert.equal(res, [])
})

test.run()

function run (name, opts) {
  const sql = readFileSync(`src/sql/${name}.sql`, 'utf8')
  return runSQL(sql, opts)
}

function runSQL (sql, opts = {}) {
  if (opts.exec) return db.exec(sql)
  let stmt = db.prepare(sql)
  if (opts.raw) stmt = stmt.raw()
  if (opts.pluck) stmt = stmt.pluck()
  if (opts.run) return stmt.run(opts.run)
  if (opts.get) return stmt.get(opts.get)
  if (opts.all) return stmt.all(opts.all)
}
