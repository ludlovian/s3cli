import { rmdir, mkdir, rename as fsRename } from 'fs/promises'
import { dirname } from 'path'

import log from 'logjs'

import { moveLocalFile } from '../db/sql.mjs'

export default async function rename (from, to, opts = {}) {
  const { dryRun } = opts
  if (dryRun) {
    log(log.blue(from.path))
    log(log.cyan(` -> ${to.path} renamed (dryrun)`))
    return
  }

  await mkdir(dirname(to.path), { recursive: true })
  await fsRename(from.path, to.path)
  let dir = dirname(from.path)
  while (true) {
    try {
      await rmdir(dir)
    } catch (err) {
      if (err.code === 'ENOTEMPTY') break
      throw err
    }
    dir = dirname(dir)
  }
  log(log.blue(from.path))
  log(log.cyan(` -> ${to.path} renamed`))

  moveLocalFile({ oldPath: from.path, newPath: to.path })
}
