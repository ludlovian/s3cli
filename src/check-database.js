'use strict'

require('promise-goodies')()
const Database = require('jsdbd')
const fs = require('fs')
const s3js = require('s3js')
const dryrun = process.argv.includes('-n')

async function checkFiles () {
  console.log('scanning files...')
  const db = new Database('file_md5_cache.db')
  const recs = await db.getAll()
  await Promise.map(recs, checkPath, { concurrency: 10 })
  await db.compact({ sorted: 'path' })
  console.log(`${recs.length.toLocaleString()} files scanned`)

  function checkPath (rec) {
    return fs.promises
      .stat(rec.path)
      .then(() => undefined)
      .catchif({ code: 'ENOENT' }, deleteRecord.bind(null, rec))
  }

  function deleteRecord (rec) {
    console.log(`${rec.path} - deleting`)
    if (dryrun) return
    return db.delete(rec)
  }
}

async function checkS3Files () {
  console.log('scanning s3 objects...')
  const db = new Database('s3file_md5_cache.db')
  const recs = await db.getAll()
  await Promise.map(recs, checkObject, { concurrency: 10 })
  await db.compact({ sorted: 'url' })
  console.log(`${recs.length.toLocaleString()} objects scanned`)

  function checkObject (rec) {
    return s3js
      .stat(`s3://${rec.url}`)
      .then(() => undefined)
      .catchif({ code: 'NotFound' }, deleteRecord.bind(null, rec))
  }

  function deleteRecord (rec) {
    console.log(`${rec.url} - deleting`)
    if (dryrun) return
    return db.delete(rec)
  }
}

checkFiles()
  .then(checkS3Files)
  .catch(console.error)
