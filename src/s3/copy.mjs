import log from 'logjs'

import { getS3 } from './util.mjs'
import { insertFile } from './sql.mjs'

export default async function copy (from, to, opts = {}) {
  const { dryRun } = opts

  if (dryRun) {
    log(log.blue(from.url))
    log(log.cyan(` -> ${to.url} copied (dryrun)`))
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

  await to.stat()
  insertFile(to)

  log(log.blue(from.url))
  log(log.cyan(` -> ${to.url} copied`))
}
