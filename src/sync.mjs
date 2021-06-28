import {
  clearSync,
  insertSyncFiles,
  selectMissingFiles,
  selectMissingHashes,
  selectChanged,
  selectSurplusFiles,
  countFiles
} from './db/index.mjs'
import cp from './cp.mjs'
import remove from './rm.mjs'
import { list, getHash } from './vfs.mjs'
import report from './report.mjs'
import { validateUrl } from './util.mjs'

export default async function sync (srcRoot, dstRoot, opts = {}) {
  srcRoot = validateUrl(srcRoot, { dir: true })
  dstRoot = validateUrl(dstRoot, { dir: true })

  clearSync()
  let scanCount = 0
  report('sync.scan.start')

  await Promise.all([
    scanFiles(srcRoot, 'src', opts.filter),
    scanFiles(dstRoot, 'dst', opts.filter)
  ])

  report('sync.scan.done')

  for (const { url, path } of selectMissingFiles()) {
    await cp(url, dstRoot + path, { ...opts, progress: true })
  }

  for (const url of selectMissingHashes()) {
    report('sync.hash', url)
    await getHash(url)
  }

  for (const { from, to } of selectChanged()) {
    await cp(from, to, { ...opts, progress: true })
  }

  if (opts.delete) {
    for (const url of selectSurplusFiles()) {
      await remove(url, opts)
    }
  }
  report('sync.done', countFiles())

  async function scanFiles (root, type, filter) {
    if (filter) {
      const r = new RegExp(filter)
      filter = x => r.test(x.path)
    }
    const lister = list(root)
    lister.on('files', files => {
      if (filter) files = files.filter(filter)
      scanCount += files.length
      report('sync.scan', { count: scanCount })
      insertSyncFiles(type, files)
    })
    await lister.done
  }
}
