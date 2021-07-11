import { getS3 } from './util.mjs'

import { sql } from '../db/index.mjs'
import { markFilesOld, insertFile, cleanFiles } from './sql.mjs'

export default async function * scan (root) {
  const File = root.constructor
  let n = 0
  const s3 = getS3()

  markFilesOld(root)
  const insertFiles = sql.transaction(files => {
    files.forEach(file => insertFile(file))
  })

  const request = { Bucket: root.bucket, Prefix: root.path }
  while (true) {
    const result = await s3.listObjectsV2(request).promise()
    const files = []
    for (const item of result.Contents) {
      const file = new File({
        type: 's3',
        bucket: root.bucket,
        path: item.Key,
        size: item.Size,
        mtime: item.LastModified,
        storage: item.StorageClass || 'STANDARD',
        md5Hash: item.ETag
      })
      if (!file.hasStats) await file.stat()
      files.push(file)
    }
    n += files.length
    insertFiles(files)
    yield n

    if (!result.IsTruncated) break
    request.ContinuationToken = result.NextContinuationToken
  }

  cleanFiles()
}
