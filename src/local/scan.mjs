import { readdir } from 'fs/promises'
import { join } from 'path'

import db from '../db.mjs'

export default async function * scan (root) {
  const File = root.constructor

  let n = 0
  const old = new Set(db.getLocalFiles(root).map(f => f.path))

  for await (const files of scanDir(root.path, File)) {
    n += files.length
    db.insertLocalFiles(files)
    files.forEach(f => old.delete(f.path))
    yield n
  }
  db.deleteLocalFiles([...old].map(path => ({ path })))
}

async function * scanDir (dir, File) {
  const entries = await readdir(dir, { withFileTypes: true })
  const files = []
  const dirs = []
  for (const entry of entries) {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) dirs.push(path)
    if (!entry.isFile()) continue
    const file = new File({ type: 'local', path })
    await file.stat()
    files.push(file)
  }
  if (files.length) yield files
  for (const dir of dirs) {
    yield * scanDir(dir, File)
  }
}
