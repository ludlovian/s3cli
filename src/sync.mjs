import {
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

export default async function sync (srcRoot, dstRoot, opts) {
  srcRoot = validateUrl(srcRoot, { dir: true })
  dstRoot = validateUrl(dstRoot, { dir: true })

  await scanFiles(srcRoot, 'src', 'source')
  await scanFiles(dstRoot, 'dst', 'destination')
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
}

async function scanFiles (root, type, desc) {
  report('sync.scan.start', { kind: desc })
  let count = 0
  const lister = list(root)
  lister.on('files', files => {
    count += files.length
    report('sync.scan', { kind: desc, count })
    insertSyncFiles(type, files)
  })
  await lister.done
}
