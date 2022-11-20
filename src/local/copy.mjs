import { mkdir, copyFile, utimes } from 'fs/promises'
import { dirname } from 'path'

import log from 'logjs'

import db from '../db.mjs'

export default async function copy (from, to, opts = {}) {
  const { dryRun } = opts
  if (dryRun) {
    log(log.blue(from.path))
    log(log.cyan(` -> ${to.path} copied (dryrun)`))
    return
  }

  await mkdir(dirname(to.path), { recursive: true })
  await copyFile(from.path, to.path)
  if (typeof from.mtime === 'string') from.mtime = new Date(from.mtime + 'Z')
  await utimes(to.path, from.mtime, from.mtime)

  log(log.blue(from.path))
  log(log.cyan(` -> ${to.path} copied`))

  db.insertLocalFiles([{ ...from, path: to.path }])
}
