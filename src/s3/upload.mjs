import { createReadStream } from 'fs'
import { PassThrough } from 'stream'
import { pipeline } from 'stream/promises'

import createSpeedo from 'speedo/gen'
import throttler from 'throttler/gen'
import progressStream from 'progress-stream/gen'

import log from 'logjs'

import { getS3, onProgress } from './util.mjs'
import db from '../db.mjs'

export default async function upload (source, dest, opts) {
  const { path, size, contentType, md5Hash } = source
  const { dryRun, limit, progress, interval = 1000 } = opts
  if (dryRun) {
    log.colour('cyan')('%s uploaded (dryrun)', dest.url)
    return
  }

  if (progress) log(log.cyan(dest.url))

  const speedo = createSpeedo({ total: size })
  const body = new PassThrough()

  const pPipeline = pipeline(
    ...[
      createReadStream(path),
      limit && throttler(limit),
      progress && speedo,
      progress && progressStream({ onProgress, interval, speedo }),
      body
    ].filter(Boolean)
  )

  const request = {
    Body: body,
    Bucket: dest.bucket,
    Key: dest.path,
    ContentLength: size,
    ContentType: contentType,
    ContentMD5: Buffer.from(md5Hash, 'hex').toString('base64'),
    Metadata: makeMetadata(source)
  }

  // perform the upload
  const s3 = getS3()
  const pUpload = s3.putObject(request).promise()

  // wait for everything to finish
  await Promise.all([pPipeline, pUpload])
  const { ETag } = await pUpload

  // check the etag is the md5 of the source data
  /* c8 ignore next 3 */
  if (ETag !== `"${md5Hash}"`) {
    throw new Error(`Upload of ${path} to ${dest} failed`)
  }

  dest.md5Hash = undefined // force re-stat
  await dest.stat()
  db.insertS3Files([dest])
}

function makeMetadata ({ mtime, size, md5Hash, contentType }) {
  const ms = new Date(mtime + 'Z').getTime()
  let md = {}
  md = { ...md, uname: 'alan', gname: 'alan', uid: 1000, gid: 1000 }
  md = { ...md, atime: ms, ctime: ms, mtime: ms }
  md = { ...md, size, mode: 0o644, md5: md5Hash, contentType }
  return {
    's3cmd-attrs': Object.keys(md)
      .sort()
      .map(k => `${k}:${md[k]}`)
      .join('/')
  }
}
