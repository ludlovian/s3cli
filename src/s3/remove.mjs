import log from 'logjs'

import { removeS3file } from '../db/sql.mjs'
import { getS3 } from './util.mjs'

export default async function remove (file, opts) {
  const { dryRun } = opts

  if (dryRun) {
    log(log.cyan(`${file.url} removed (dryrun)`))
    return
  }

  const s3 = getS3()
  await s3.deleteObject({ Bucket: file.bucket, Key: file.path }).promise()
  removeS3file(file)
  log(log.cyan(`${file.url} removed`))
}
