'use strict'

import { join } from 'path'

import Local from './local'
import Remote from './remote'
import log from './log'
import match from './match'
import { comma, retry } from './util'
import upload from './upload'
import download from './download'

export default async function sync (
  lRoot,
  rRoot,
  { dryRun, download: downsync, delete: deleteExtra, ...options }
) {
  lRoot = lRoot.replace(/\/$/, '')
  rRoot = rRoot.replace(/\/$/, '')

  log.status('Scanning files')

  const filter = getFilter(options)
  const lFiles = Local.scan(lRoot, filter)
  const rFiles = Remote.scan(rRoot, filter)

  let fileCount = 0
  for await (const [local, remote] of match('path', lFiles, rFiles)) {
    fileCount++
    const path = local ? local.path : remote.path
    log.status(path)
    if (local) {
      local.on('hashing', () => log.status(`${local.path} - hashing`))
      await local.getHash()
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
          log(`${path} - local delete`)
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
          log(`${path} - remote delete`)
        }
      }
    }
  }
  log(`${comma(fileCount)} files processed.`)

  function uploadFile ({ path, fullpath }) {
    if (dryRun) return log(`${path} - upload (dry run)`)

    return retry(() =>
      upload(fullpath, `${rRoot}/${path}`, {
        ...options,
        progress: true
      })
    )
  }

  function downloadFile ({ path, url }) {
    if (dryRun) return log(`${path} - download (dry run)`)

    return retry(() =>
      download(url, join(lRoot, path), {
        ...options,
        progress: true
      })
    )
  }
}

function getFilter ({ filter }) {
  if (!filter) return () => true
  const rgx = new RegExp(filter)
  return x => rgx.test(x)
}
