import log from 'logjs'

import { moveS3file } from '../db/sql.mjs'
import { getS3 } from './util.mjs'

export default async function rename (from, to, opts = {}) {
  const { dryRun } = opts

  if (dryRun) {
    log(log.blue(from.url))
    log(log.cyan(` -> ${to.url} renamed (dryrun)`))
    return
  }

  const s3 = getS3()
  await s3
    .copyObject({
      Bucket: to.bucket,
      Key: to.path,
      CopySource: `${from.bucket}/${from.path}`,
      MetadataDirective: 'COPY'
    })
    .promise()

  await s3
    .deleteObject({
      Bucket: from.bucket,
      Key: from.path
    })
    .promise()
  log(log.blue(from.url))
  log(log.cyan(` -> ${to.url} renamed`))
  moveS3file({
    bucket: from.bucket,
    oldPath: from.path,
    newPath: to.path
  })
}
