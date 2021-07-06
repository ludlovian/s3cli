import File from './lib/file.mjs'
import s3remove from './s3/remove.mjs'
import localRemove from './local/remove.mjs'

export default async function rm (file, opts = {}) {
  file = File.fromUrl(file, { resolve: true })
  await file.stat()
  if (file.isS3) {
    await s3remove(file, opts)
  } else {
    await localRemove(file, opts)
  }
}
