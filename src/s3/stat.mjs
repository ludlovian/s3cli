import { getS3 } from './util.mjs'

const MD_KEY = 's3cmd-attrs'

export default async function stat (file) {
  const s3 = getS3()
  const req = { Bucket: file.bucket, Key: file.path }
  const item = await s3.headObject(req).promise()
  file.mtime = item.LastModified
  file.size = item.ContentLength
  file.storage = item.StorageClass || 'STANDARD'
  if (item.Metadata && item.Metadata[MD_KEY]) {
    file.metadata = unpack(item.Metadata[MD_KEY])
  }
  if (!item.ETag.includes('-')) {
    file.md5Hash = item.ETag.replaceAll('"', '')
  } else if (file.metadata && file.metadata.md5) {
    file.md5Hash = file.metadata.md5
  } else {
    throw new Error('Could not get md5 hash for ' + file.url)
  }
}

function unpack (s) {
  return Object.fromEntries(
    s
      .split('/')
      .map(x => x.split(':'))
      .map(([k, v]) => {
        if (!isNaN(Number(v))) v = Number(v)
        if (k.endsWith('time')) v = new Date(v)
        return [k, v]
      })
  )
}
