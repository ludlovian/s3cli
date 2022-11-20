import log from 'logjs'

import db from '../db.mjs'
import { getS3 } from './util.mjs'

export default async function remove (file, opts) {
  const { dryRun } = opts

  if (dryRun) {
    log(log.cyan(`${file.url} removed (dryrun)`))
    return
  }

  const s3 = getS3()
  await s3.deleteObject({ Bucket: file.bucket, Key: file.path }).promise()
  db.deleteS3Files([file])
  log(log.cyan(`${file.url} removed`))
}
