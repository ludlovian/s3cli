import { lstat } from 'fs/promises'

import hashFile from 'hash-stream/simple'
import log from 'logjs'

import db from '../db.mjs'

export default async function stat (file) {
  const stats = await lstat(file.path)
  file.size = stats.size
  file.mtime = stats.mtime
  const f = db.locateLocalFile(file)
  if (f) file.md5Hash = f.md5Hash
  if (!file.md5Hash) {
    log.status('%s ... hashing', file.path)
    file.md5Hash = await hashFile(file.path)
    db.insertLocalFiles([file])
  }
}
