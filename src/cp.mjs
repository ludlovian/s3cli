import log from 'logjs'

import File from './lib/file.mjs'
import s3upload from './s3/upload.mjs'
import s3download from './s3/download.mjs'
import gdriveDownload from './drive/download.mjs'
import { getDirection } from './util.mjs'
import db from './db.mjs'

export default async function cp (src, dst, opts = {}) {
  db.open()
  src = File.fromUrl(src, { resolve: true })
  dst = File.fromUrl(dst, { resolve: true })
  const dir = getDirection(src, dst)
  const fns = {
    local_s3: s3upload,
    s3_local: s3download,
    gdrive_local: gdriveDownload
  }
  const fn = fns[dir]

  await src.stat()
  await fn(src, dst, opts)

  if (!opts.dryRun && !opts.progress && !opts.quiet) log(dst.url)
}
