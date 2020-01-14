'use strict'

const Database = require('jsdbd')
const fs = require('fs')
const s3js = require('s3js')
const dryrun = process.argv.includes('-n')

async function checkFiles () {
  console.log('scanning files...')
  const db = new Database('file_md5_cache.db')
  const recs = await db.getAll()
  await Promise.all(recs.map(checkPath))
  await db.compact({ sorted: 'path' })
  console.log('done')

  async function checkPath (rec) {
    const { path } = rec
    try {
      await fs.promises.stat(path)
      return
    } catch (err) {
      if (err.code !== 'ENOENT') throw err
    }
    console.log(`${path} - deleting`)
    if (dryrun) return
    return db.delete(rec)
  }
}

async function checkS3Files () {
  console.log('scanning s3 files...')
  const db = new Database('s3file_md5_cache.db')
  const recs = await db.getAll()
  await Promise.all(recs.map(checkObject))
  await db.compact({ sorted: 'url' })
  console.log('done')

  async function checkObject (rec) {
    const { url } = rec
    try {
      await s3js.stat(`s3://${url}`)
      return
    } catch (err) {
      if (err.code !== 'NotFound') throw err
    }
    console.log(`${url} - deleting`)
    if (dryrun) return
    return db.delete(rec)
  }
}

checkFiles()
  .then(checkS3Files)
  .catch(console.error)
