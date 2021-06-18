import { pipeline } from 'stream/promises'

import throttler from 'throttler/gen'
import progressStream from 'progress-stream/gen'
import createSpeedo from 'speedo/gen'

import * as s3 from './lib/s3.mjs'
import * as local from './lib/local.mjs'
import { deleteHash } from './db/index.mjs'

export function list (url) {
  if (isS3(url)) return s3.list(url)
  else if (isLocal(url)) return local.list(url)
  /* c8 ignore next */ else throw new Error('Huh? ' + url)
}

export async function stat (url) {
  if (isS3(url)) return s3.stat(url)
  else if (isLocal(url)) return local.stat(url)
  /* c8 ignore next */ else throw new Error('Huh? ' + url)
}

export async function copy (srcUrl, dstUrl, opts = {}) {
  const { onProgress, limit } = opts
  const sourceStream = isS3(srcUrl)
    ? await s3.createReadStream(srcUrl)
    : await local.createReadStream(srcUrl)

  const { source } = sourceStream

  const destStream = isS3(dstUrl)
    ? await s3.createWriteStream(dstUrl, source)
    : await local.createWriteStream(dstUrl, source)

  const speedo = createSpeedo({ total: source.size })

  const pPipeline = pipeline(
    [
      sourceStream,
      limit && throttler(limit),
      onProgress && speedo,
      onProgress && progressStream({ onProgress, speedo }),
      destStream
    ].filter(Boolean)
  )

  await Promise.all([pPipeline, destStream.done])
}

export function getHash (url) {
  /* c8 ignore next */
  if (isS3(url)) return s3.getHash(url)
  else if (isLocal(url)) return local.getHash(url)
  /* c8 ignore next */ else throw new Error('Huh? ' + url)
}

export async function remove (url) {
  if (isS3(url)) await s3.remove(url)
  else if (isLocal(url)) await local.remove(url)
  /* c8 ignore next */ else throw new Error('Huh? ' + url)
  deleteHash({ url })
}

function isS3 (url) {
  return url.startsWith('s3://')
}

function isLocal (url) {
  return url.startsWith('file:///')
}
