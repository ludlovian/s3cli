import EventEmitter from 'events'
import { PassThrough } from 'stream'
import AWS from 'aws-sdk'

import once from 'pixutil/once'

import { insertHash } from '../db/index.mjs'

const getS3 = once(async () => {
  const REGION = 'eu-west-1'
  return new AWS.S3({ region: REGION })
})

function parseAddress (url) {
  const m = /^s3:\/\/([^/]+)(?:\/(.*))?$/.exec(url)
  if (!m) throw new TypeError(`Bad S3 URL: ${url}`)
  return { Bucket: m[1], Key: m[2] || '' }
}

export function list (baseurl) {
  if (!baseurl.endsWith('/')) baseurl = baseurl + '/'
  const { Bucket, Key: Prefix } = parseAddress(baseurl)
  const lister = new EventEmitter()
  lister.done = (async () => {
    const s3 = await getS3()
    const request = { Bucket, Prefix }
    while (true) {
      const result = await s3.listObjectsV2(request).promise()
      const files = result.Contents.map(item => {
        const file = {}
        file.url = `s3://${Bucket}/${item.Key}`
        file.path = file.url.slice(baseurl.length)
        file.size = item.Size
        file.mtime = item.LastModified
        if (!item.ETag.includes('-')) file.md5 = item.ETag.replaceAll('"', '')
        file.storage = item.StorageClass
        return file
      })
      if (files.length) lister.emit('files', files)
      if (!result.IsTruncated) break
      request.ContinuationToken = result.NextContinuationToken
    }
    lister.emit('done')
  })()
  return lister
}

export async function stat (url) {
  const { Bucket, Key } = parseAddress(url)
  const s3 = await getS3()

  const request = { Bucket, Key }
  const res = await s3.headObject(request).promise()
  const attrs = unpackMetadata(res.Metadata)
  return {
    contentType: res.ContentType,
    mtime: res.LastModified,
    size: res.ContentLength,
    md5: !res.ETag.includes('-') ? res.ETag.replaceAll('"', '') : attrs.md5,
    storage: res.StorageClass,
    attrs
  }
}

export async function getHash (url) {
  const { mtime, size, attrs } = await stat(url)
  const hash = attrs && attrs.md5 ? attrs.md5 : null
  insertHash({ url, mtime, size, hash })
  return hash
}

export async function createReadStream (url) {
  const { Bucket, Key } = parseAddress(url)
  const s3 = await getS3()
  const { mtime, size, contentType, hash, attrs } = await stat(url)
  const source = { mtime, size, contentType, hash, attrs }

  const stream = s3.getObject({ Bucket, Key }).createReadStream()
  stream.source = source
  return stream
}

export async function createWriteStream (url, source) {
  const { size, contentType, hash, attrs } = source
  const { Bucket, Key } = parseAddress(url)
  const s3 = await getS3()
  const passthru = new PassThrough()
  if (!hash) throw new Error('No hash supplied')
  if (!size) throw new Error('No size supplied')
  if (!contentType) throw new Error('No contentType supplied')

  const request = {
    Body: passthru,
    Bucket,
    Key,
    ContentLength: size,
    ContentType: contentType,
    ContentMD5: Buffer.from(hash, 'hex').toString('base64'),
    Metadata: packMetadata(attrs)
  }

  passthru.done = s3
    .putObject(request)
    .promise()
    .then(() => getHash(url))
  return passthru
}

export async function remove (url) {
  const { Bucket, Key } = parseAddress(url)
  const s3 = await getS3()
  await s3.deleteObject({ Bucket, Key }).promise()
}

function unpackMetadata (md, key = 's3cmd-attrs') {
  const numbers = new Set(['mode', 'size', 'gid', 'uid'])
  const dates = new Set(['atime', 'mtime', 'ctime'])
  if (!md || typeof md !== 'object' || !md[key]) return {}
  return Object.fromEntries(
    md[key]
      .split('/')
      .map(x => x.split(':'))
      .map(([k, v]) => {
        if (dates.has(k)) {
          v = new Date(Number(v))
        } else if (numbers.has(k)) {
          v = Number(v)
        }
        return [k, v]
      })
  )
}

function packMetadata (obj, key = 's3cmd-attrs') {
  return {
    [key]: Object.keys(obj)
      .sort()
      .map(k => [k, obj[k]])
      .filter(([k, v]) => v != null)
      .map(([k, v]) => `${k}:${v instanceof Date ? +v : v}`)
      .join('/')
  }
}
