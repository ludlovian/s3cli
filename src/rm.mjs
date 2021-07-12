import File from './lib/file.mjs'
import s3remove from './s3/remove.mjs'
import localRemove from './local/remove.mjs'

export default async function rm (file, opts = {}) {
  file = File.fromUrl(file, { resolve: true })
  const fns = {
    local: localRemove,
    s3: s3remove
  }
  const fn = fns[file.type]

  await file.stat()
  await fn(file, opts)
}
