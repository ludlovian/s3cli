import { getS3 } from './util.mjs'

export default async function * scan (root) {
  const File = (await import('../lib/file.mjs')).default
  const s3 = getS3()
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
    yield files

    if (!result.IsTruncated) break
    request.ContinuationToken = result.NextContinuationToken
  }
}
