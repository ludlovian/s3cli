import { unlink } from 'fs/promises'
import { join } from 'path'

import { deleteObject } from 's3js'
import retry from 'retry'

import Local from './local.mjs'
import Remote from './remote.mjs'
import match from './match.mjs'
import upload from './upload.mjs'
import download from './download.mjs'
import report from './report.mjs'

export default async function sync (
  lRoot,
  rRoot,
  { dryRun, download: downsync, delete: deleteExtra, ...options }
) {
  lRoot = lRoot.replace(/\/$/, '')
  rRoot = rRoot.replace(/\/$/, '')

  report('sync.start')

  const filter = getFilter(options)
  const lFiles = Local.scan(lRoot, filter)
  const rFiles = Remote.scan(rRoot, filter)

  let fileCount = 0
  for await (const [local, remote] of match('path', lFiles, rFiles)) {
    fileCount++
    const path = local ? local.path : remote.path
    report('sync.file.start', path)
    if (local) {
      local.on('hashing', () => report('sync.file.hashing', path))
      await local.getHash()
    }
    if (remote) {
      remote.on('hashing', () => report('sync.file.hashing', path))
      await remote.getHash()
    }
    if (local && remote) {
      // if they are the same, we can skip this file
      if (local.hash === remote.hash) continue
      if (downsync) {
        await downloadFile(remote)
      } else {
        await uploadFile(local)
      }
    } else if (local) {
      if (downsync) {
        if (deleteExtra) {
          await deleteLocal(local)
        }
      } else {
        await uploadFile(local)
      }
    } else {
      // only remote exists. If uploading, warn about extraneous files, else
      // download it
      if (downsync) {
        await downloadFile(remote)
      } else {
        if (deleteExtra) {
          await deleteRemote(remote)
        }
      }
    }
  }
  report('sync.done', { count: fileCount })

  async function uploadFile ({ path, fullpath }) {
    if (dryRun) {
      report('sync.file.dryrun', { path, action: 'upload' })
      return
    }

    return retry(
      () =>
        upload(fullpath, `${rRoot}/${path}`, {
          ...options,
          progress: true
        }),
      {
        retries: 5,
        delay: 5000,
        onRetry: data => report('retry', data)
      }
    )
  }

  async function downloadFile ({ path, url }) {
    if (dryRun) {
      report('sync.file.dryrun', { path, action: 'download' })
      return
    }

    return retry(
      () =>
        download(url, join(lRoot, path), {
          ...options,
          progress: true
        }),
      {
        retries: 5,
        delay: 5000,
        onRetry: data => report('retry', data)
      }
    )
  }

  async function deleteLocal ({ path }) {
    if (dryRun) {
      report('sync.file.dryrun', { path, action: 'delete' })
      return
    }

    return unlink(join(lRoot, path))
  }

  async function deleteRemote ({ path }) {
    if (dryRun) {
      report('sync.file.dryrun', { path, action: 'delete' })
      return
    }
    const url = `${rRoot}/${path}`
    report('delete.file.start', url)
    await deleteObject(url)
    report('delete.file.done', url)
  }
}

function getFilter ({ filter }) {
  if (!filter) return () => true
  const rgx = new RegExp(filter)
  return x => rgx.test(x)
}
