'use strict'

const Datastore = require('jsdbd')
const fs = require('fs')
const dryrun = process.argv.includes('-n')

async function main () {
  console.log('scanning...')
  const db = new Datastore('file_md5_cache.db')
  const all = await db.findAll('_id')
  const recs = all.map(item => item[1])
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

main().catch(console.error)
