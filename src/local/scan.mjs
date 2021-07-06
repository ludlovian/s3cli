import { readdir } from 'fs/promises'
import { join } from 'path'

export default async function * scan (root) {
  yield * scanDir(root.path)
}

async function * scanDir (dir) {
  const File = (await import('../lib/file.mjs')).default
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
    yield * scanDir(dir)
  }
}
