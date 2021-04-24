import { stat } from 'fs/promises'

import promiseGoodies from 'promise-goodies'
import Database from 'jsdbd'
import { stat as s3stat } from 's3js'

const dryrun = process.argv.includes('-n')
promiseGoodies()

async function checkFiles () {
  console.log('scanning files...')
  const db = new Database('file_md5_cache.db')
  const recs = await db.getAll()
  await Promise.map(recs, checkPath, { concurrency: 10 })
  await db.compact({ sorted: 'path' })
  console.log(`${recs.length.toLocaleString()} files scanned`)

  async function checkPath (rec) {
    try {
      await stat(rec.path)
    } catch (err) {
      if (err.code === 'ENOENT') return deleteRecord(rec)
      throw err
    }
  }

  async function deleteRecord (rec) {
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

  async function checkObject (rec) {
    try {
      await s3stat(`s3://${rec.url}`)
    } catch (err) {
      if (err.code === 'NotFound') return deleteRecord(rec)
      throw err
    }
  }

  async function deleteRecord (rec) {
    console.log(`${rec.url} - deleting`)
    if (dryrun) return
    return db.delete(rec)
  }
}

checkFiles()
  .then(checkS3Files)
  .catch(console.error)
