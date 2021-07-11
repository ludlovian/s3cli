import { sql } from './db/index.mjs'
import File from './lib/file.mjs'
import { isUploading } from './util.mjs'
import localCopy from './local/copy.mjs'
import localRemove from './local/remove.mjs'
import upload from './s3/upload.mjs'
import download from './s3/download.mjs'
import s3copy from './s3/copy.mjs'
import s3remove from './s3/remove.mjs'

export default async function sync (srcRoot, dstRoot, opts = {}) {
  srcRoot = File.fromUrl(srcRoot, { resolve: true, directory: true })
  dstRoot = File.fromUrl(dstRoot, { resolve: true, directory: true })
  isUploading(srcRoot, dstRoot)
  const fn = getFunctions(srcRoot, dstRoot)

  await srcRoot.scan()
  await dstRoot.scan()

  const destFiles = new Set()

  // new files
  for (const row of fn.newFiles(fn.paths)) {
    const src = File.like(srcRoot, row)
    const dst = src.rebase(srcRoot, dstRoot)
    await fn.copy(src, dst, { ...opts, progress: true })
    destFiles.add(dst.url)
  }

  // simple renames
  for (const { contentId } of fn.differences(fn.paths)) {
    const d = { contentId, ...fn.paths }
    const src = File.like(srcRoot, fn.srcContent.get(d))
    const dst = src.rebase(srcRoot, dstRoot)
    const old = File.like(dstRoot, fn.dstContent.get(d))
    if (!old.archived) {
      await fn.destCopy(old, dst, opts)
    } else {
      await fn.copy(src, dst, { ...opts, progress: true })
    }
    destFiles.add(dst.url)
  }

  // complex renames
  for (const { contentId } of fn.duplicates()) {
    const d = { contentId, ...fn.paths }
    const srcs = fn.srcContent.all(d).map(row => File.like(srcRoot, row))
    const dsts = fn.dstContent.all(d).map(row => File.like(dstRoot, row))
    for (const src of srcs) {
      const dst = src.rebase(srcRoot, dstRoot)
      if (!dsts.find(d => d.url === dst.url)) {
        await fn.copy(src, dst, { ...opts, progress: true })
        destFiles.add(dst.url)
      }
    }

    if (opts.delete) {
      for (const dst of dsts) {
        const src = dst.rebase(dstRoot, srcRoot)
        if (!srcs.find(s => s.url === src.url)) {
          await fn.remove(dst, opts)
          destFiles.add(dst.url)
        }
      }
    }
  }

  // deletes
  if (opts.delete) {
    for (const row of fn.oldFiles(fn.paths)) {
      const dst = File.like(dstRoot, row)
      if (!destFiles.has(dst.url)) {
        await fn.remove(dst, opts)
      }
    }
  }
}

function getFunctions (srcRoot, dstRoot) {
  const paths = {
    localPath:
      (srcRoot.isLocal && srcRoot.path) || (dstRoot.isLocal && dstRoot.path),
    s3Bucket:
      (srcRoot.isS3 && srcRoot.bucket) || (dstRoot.isS3 && dstRoot.bucket),
    s3Path: (srcRoot.isS3 && srcRoot.path) || (dstRoot.isS3 && dstRoot.path)
  }
  if (srcRoot.isLocal) {
    return {
      paths,
      newFiles: listLocalNotRemote.all,
      oldFiles: listRemoteNotLocal.all,
      differences: listDifferences.all,
      duplicates: listDuplicates.all,
      srcContent: findLocalContent,
      dstContent: findRemoteContent,
      copy: upload,
      destCopy: s3copy,
      remove: s3remove
    }
  } else {
    return {
      paths,
      newFiles: listRemoteNotLocal.all,
      oldFiles: listLocalNotRemote.all,
      differences: listDifferences.all,
      duplicates: listDuplicates.all,
      srcContent: findRemoteContent,
      dstContent: findLocalContent,
      copy: download,
      destCopy: localCopy,
      remove: localRemove
    }
  }
}

const listLocalNotRemote = sql(`
  SELECT * FROM local_file_view
  WHERE path LIKE $localPath || '%'
  AND contentId NOT IN (
    SELECT contentId
    FROM s3_file
    WHERE bucket = $s3Bucket
    AND   path LIKE $s3Path || '%'
  )
`)

const listRemoteNotLocal = sql(`
  SELECT * FROM s3_file_view
  WHERE bucket = $s3Bucket
  AND   path LIKE $s3Path || '%'
  AND   contentId NOT IN (
    SELECT contentId
    FROM local_file
    WHERE path LIKE $localPath || '%'
  )
`)

const listDifferences = sql(`
  SELECT * FROM local_and_s3_view
  WHERE localPath LIKE $localPath || '%'
  AND   s3Bucket = $s3Bucket
  AND   s3Path LIKE $s3Path || '%'
  AND   substr(localPath, 1 + length($localPath)) !=
          substr(s3Path, 1 + length($s3Path))
`)

const listDuplicates = sql(`
  SELECT contentId FROM duplicates_view
`)

const findLocalContent = sql(`
  SELECT * FROM local_file_view
  WHERE contentId = $contentId
  AND   path LIKE $localPath || '%'
`)

const findRemoteContent = sql(`
  SELECT * FROM s3_file_view
  WHERE contentId = $contentId
  AND   bucket = $s3Bucket
  AND   path LIKE $s3Path || '%'
`)
