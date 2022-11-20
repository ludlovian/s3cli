import { getS3 } from './util.mjs'

import db from '../db.mjs'

export default async function * scan (root) {
  const File = root.constructor
  const { bucket } = root
  let n = 0
  const s3 = getS3()

  const old = new Set(db.getS3Files(root).map(r => r.path))

  const request = { Bucket: bucket, Prefix: root.path }
  while (true) {
    const result = await s3.listObjectsV2(request).promise()
    const files = []
    for (const item of result.Contents) {
      const file = new File({
        type: 's3',
        bucket,
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
    db.insertS3Files(files)
    files.forEach(f => old.delete(f.path))
    yield n

    if (!result.IsTruncated) break
    request.ContinuationToken = result.NextContinuationToken
  }

  db.deleteS3Files([...old].map(path => ({ bucket, path })))
}
