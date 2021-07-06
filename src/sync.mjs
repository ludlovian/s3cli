import log from 'logjs'

import {
  cleanup,
  findDuplicates,
  findLocalNotRemote,
  findRemoteNotLocal,
  findDifferentPaths
} from './db/sql.mjs'
import File from './lib/file.mjs'
import { isUploading } from './util.mjs'
import localRename from './local/rename.mjs'
import localRemove from './local/remove.mjs'
import upload from './s3/upload.mjs'
import download from './s3/download.mjs'
import s3rename from './s3/rename.mjs'
import s3remove from './s3/remove.mjs'

export default async function sync (srcRoot, dstRoot, opts = {}) {
  srcRoot = File.fromUrl(srcRoot, { resolve: true, directory: true })
  dstRoot = File.fromUrl(dstRoot, { resolve: true, directory: true })
  const uploading = isUploading(srcRoot, dstRoot)

  await srcRoot.scan()
  await dstRoot.scan()
  checkDuplicates()

  const updatedFiles = new Set()

  const roots = {
    localRoot: uploading ? srcRoot.url : dstRoot.url,
    s3Root: uploading ? dstRoot.url : srcRoot.url
  }

  // add in new from source
  const sql = uploading ? findLocalNotRemote : findRemoteNotLocal

  for (const row of sql.all(roots)) {
    const type = uploading ? 'local' : 's3'
    const src = new File({ type, ...row })
    const dest = src.rebase(srcRoot, dstRoot)
    if (uploading) {
      await upload(src, dest, { ...opts, progress: true })
    } else {
      await download(src, dest, { ...opts, progress: true })
    }
    updatedFiles.add(dest.url)
  }

  // rename files on destination
  for (const row of findDifferentPaths.all(roots)) {
    const { local, remote } = getDifferentFiles(row, srcRoot, dstRoot)
    if (uploading) {
      const dest = local.rebase(srcRoot, dstRoot)
      if (remote.storage.toLowerCase().startsWith('standard')) {
        await s3rename(remote, dest, opts)
      } else {
        await upload(local, dest, { ...opts, progress: true })
        await s3remove(remote)
      }
      updatedFiles.add(dest.url)
    } else {
      const dest = remote.rebase(srcRoot, dstRoot)
      await localRename(local, dest, opts)
      updatedFiles.add(dest.url)
    }
  }

  // delete extra from destination
  if (opts.delete) {
    const sql = uploading ? findRemoteNotLocal : findLocalNotRemote
    const type = uploading ? 's3' : 'local'
    for (const row of sql.all(roots)) {
      const file = new File({ type, ...row })
      if (updatedFiles.has(file.url)) continue
      if (uploading) {
        await s3remove(file, opts)
      } else {
        await localRemove(file, opts)
      }
    }
  }
  cleanup()
}

function checkDuplicates () {
  const dups = findDuplicates.all()
  if (!dups.length) return

  log('\nDUPLICATES FOUND')
  for (const { contentId, url } of dups) {
    log('%d - %s', contentId, url)
  }
  process.exit()
}

function getDifferentFiles (row, srcRoot, dstRoot) {
  const localRoot = srcRoot.isLocal ? srcRoot : dstRoot
  const remoteRoot = srcRoot.isS3 ? srcRoot : dstRoot
  const local = new File({
    type: 'local',
    path: localRoot.path + row.localPath,
    mtime: row.localMtime,
    size: row.size,
    contentType: row.contentType,
    md5Hash: row.md5Hash
  })
  const remote = new File({
    type: 's3',
    bucket: remoteRoot.bucket,
    path: remoteRoot.path + row.remotePath,
    storage: row.storage,
    mtime: row.remoteMtime,
    size: row.size,
    contentType: row.contentType,
    md5Hash: row.md5Hash
  })
  return { local, remote }
}
