import { createWriteStream } from 'fs'
import { utimes, mkdir } from 'fs/promises'
import { pipeline } from 'stream/promises'
import { dirname } from 'path'

import createSpeedo from 'speedo/gen'
import throttler from 'throttler/gen'
import progressStream from 'progress-stream/gen'
import hashStream from 'hash-stream/gen'
import log from 'logjs'

import getDriveAPI from './api.mjs'
import { onProgress } from '../s3/util.mjs'
import db from '../db.mjs'

export default async function download (src, dst, opts = {}) {
  const { size, mtime, md5Hash, googleId } = src
  const { dryRun, progress, interval = 1000, limit } = opts

  if (dryRun) {
    log.colour('cyan')('%s downloaded (dryrun)', dst.path)
    return
  }

  if (progress) log(log.cyan(dst.path))

  await mkdir(dirname(dst.path), { recursive: true })

  const drive = await getDriveAPI()
  const hasher = hashStream()
  const speedo = createSpeedo({ total: size })
  const req = { fileId: googleId, alt: 'media' }
  const reqOpts = { responseType: 'stream' }
  const streams = [
    (await drive.files.get(req, reqOpts)).data,
    hasher,
    limit && throttler(limit),
    progress && speedo,
    progress && progressStream({ onProgress, interval, speedo }),
    createWriteStream(dst.path)
  ].filter(Boolean)

  await pipeline(...streams)

  if (mtime) await utimes(dst.path, mtime, mtime)
  db.insertLocalFiles([{ ...dst, size, mtime, md5Hash }])
}
