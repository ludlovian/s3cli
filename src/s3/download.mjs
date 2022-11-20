import { createWriteStream } from 'fs'
import { utimes, mkdir } from 'fs/promises'
import { pipeline } from 'stream/promises'
import { dirname } from 'path'

import createSpeedo from 'speedo/gen'
import throttler from 'throttler/gen'
import hashStream from 'hash-stream/gen'
import progressStream from 'progress-stream/gen'

import log from 'logjs'

import db from '../db.mjs'
import { getS3, onProgress } from './util.mjs'

export default async function download (source, dest, opts = {}) {
  const { bucket, path, size, mtime, md5Hash, storage } = source
  const { dryRun, progress, interval = 1000, limit } = opts

  if (!storage.toLowerCase().startsWith('standard')) {
    throw new Error(`${source.url} needs to be restored for copy`)
  }

  if (dryRun) {
    log.colour('cyan')('%s downloaded (dryrun)', dest.path)
    return
  }

  if (progress) log(log.cyan(dest.path))

  await mkdir(dirname(dest.path), { recursive: true })

  const s3 = getS3()
  const hasher = hashStream()
  const speedo = createSpeedo({ total: size })
  const streams = [
    s3.getObject({ Bucket: bucket, Key: path }).createReadStream(),
    hasher,
    limit && throttler(limit),
    progress && speedo,
    progress && progressStream({ onProgress, interval, speedo }),
    createWriteStream(dest.path)
  ].filter(Boolean)

  await pipeline(...streams)
  /* c8 ignore next 3 */
  if (hasher.hash !== md5Hash) {
    throw new Error(`Error downloading ${source.url} to ${dest.path}`)
  }

  const tm = new Date(mtime + 'Z')
  await utimes(dest.path, tm, tm)

  db.insertLocalFiles([{ ...dest, md5Hash, size, mtime }])
}
