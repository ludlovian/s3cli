import { readdir } from 'fs/promises'
import { join } from 'path'

import { sql } from '../db/index.mjs'
import { listFiles, insertFile, removeFile } from './sql.mjs'

export default async function * scan (root) {
  const File = root.constructor

  let n = 0
  const old = new Set(listFiles.pluck().all(root))

  const insertFiles = sql.transaction(files => {
    for (const file of files) {
      insertFile(file)
      old.delete(file.path)
    }
  })
  const deleteOld = sql.transaction(paths => {
    for (const path of paths) {
      removeFile({ path })
    }
  })

  for await (const files of scanDir(root.path, File)) {
    n += files.length
    insertFiles(files)
    yield n
  }
  deleteOld([...old])
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
