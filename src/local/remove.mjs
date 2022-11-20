import { rmdir, unlink } from 'fs/promises'
import { dirname } from 'path'

import log from 'logjs'

import db from '../db.mjs'

export default async function remove (file, opts) {
  const { dryRun } = opts
  if (dryRun) {
    log(log.cyan(`${file.path} deleted (dryrun)`))
    return
  }
  await unlink(file.path)
  let dir = dirname(file.path)
  while (true) {
    try {
      await rmdir(dir)
    } catch (err) {
      if (err.code === 'ENOTEMPTY') break
      throw err
    }
    dir = dirname(dir)
  }
  db.deleteLocalFiles([file])
  log(log.cyan(`${file.path} deleted`))
}
