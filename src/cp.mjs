import log from 'logjs'

import File from './lib/file.mjs'
import upload from './s3/upload.mjs'
import download from './s3/download.mjs'
import { isUploading } from './util.mjs'

export default async function cp (src, dst, opts = {}) {
  src = File.fromUrl(src, { resolve: true })
  dst = File.fromUrl(dst, { resolve: true })
  const uploading = isUploading(src, dst)

  await src.stat()

  if (uploading) {
    await upload(src, dst, opts)
  } else {
    await download(src, dst, opts)
  }

  if (!opts.dryRun && !opts.progress && !opts.quiet) log(dst.url)
}
