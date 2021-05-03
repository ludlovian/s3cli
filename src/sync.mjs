import { unlink, realpath } from 'fs/promises'
import { join, resolve } from 'path'

import weave from 'weave'

import Local from './local.mjs'
import Remote from './remote.mjs'
import upload from './upload.mjs'
import download from './download.mjs'
import rm from './rm.mjs'
import report from './report.mjs'
import { removeRow, compactDatabase } from './database.mjs'

export default async function sync (
  lRoot,
  rRoot,
  { dryRun, download: downsync, delete: deleteExtra, ...options }
) {
  report('sync.start')
  lRoot = await realpath(resolve(lRoot.replace(/\/$/, '')))
  rRoot = rRoot.replace(/\/$/, '')

  const filter = getFilter(options)
  const lFiles = Local.files(lRoot, filter)
  const lHashes = Local.hashes(lRoot, filter)
  const rFiles = Remote.files(rRoot, filter)
  const rHashes = Remote.hashes(rRoot, filter)

  let fileCount = 0
  const items = weave('path', lFiles, lHashes, rFiles, rHashes)

  for await (const item of items) {
    fileCount++
    const [path, local, lrow, remote, rrow] = item
    if (path) report('sync.file.start', path)

    if (local) {
      local.on('hashing', () => report('sync.file.hashing', path))
      await local.getHash(lrow)
    }

    if (remote) {
      remote.on('hashing', () => report('sync.file.hashing', path))
      await remote.getHash(rrow)
    }

    if (local && remote) {
      if (local.hash === remote.hash) continue
      if (downsync) {
        await downloadFile(remote.url, lRoot, path)
      } else {
        await uploadFile(local.fullpath, rRoot, path)
      }
    } else if (local) {
      if (downsync) {
        if (deleteExtra) {
          await deleteLocal(lRoot, path)
        }
      } else {
        await uploadFile(local.fullpath, rRoot, path)
      }
    } else if (remote) {
      if (downsync) {
        await downloadFile(remote.url, lRoot, path)
      } else {
        if (deleteExtra) {
          await deleteRemote(rRoot, path)
        }
      }
    } else {
      if (lrow) await removeRow(lrow)
      if (rrow) await removeRow(rrow)
    }
  }
  await compactDatabase()

  report('sync.done', { count: fileCount })

  async function uploadFile (file, rRoot, path, action = 'upload') {
    if (dryRun) return report('sync.file.dryrun', { path, action })
    return upload(file, `${rRoot}/${path}`, { ...options, progress: true })
  }

  async function downloadFile (url, lRoot, path, action = 'download') {
    if (dryRun) return report('sync.file.dryrun', { path, action })
    return download(url, join(lRoot, path), { ...options, progress: true })
  }

  async function deleteLocal (lRoot, path, action = 'delete') {
    if (dryRun) return report('sync.file.dryrun', { path, action })
    return unlink(join(lRoot, path))
  }

  async function deleteRemote (rRoot, path, action = 'delete') {
    if (dryRun) return report('sync.file.dryrun', { path, action })
    return rm(`${rRoot}/${path}`)
  }
}

function getFilter ({ filter }) {
  if (!filter) return () => true
  const rgx = new RegExp(filter)
  return x => rgx.test(x)
}
