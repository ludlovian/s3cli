import { readdir } from 'fs/promises'
import { join } from 'path'

import { sql } from '../db/index.mjs'
import { markFilesOld, insertFile, cleanFiles } from './sql.mjs'

export default async function * scan (root) {
  const File = root.constructor

  let n = 0
  markFilesOld(root)
  const insertFiles = sql.transaction(files => {
    files.forEach(file => insertFile(file))
  })

  for await (const files of scanDir(root.path, File)) {
    n += files.length
    insertFiles(files)
    yield n
  }
  cleanFiles()
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
