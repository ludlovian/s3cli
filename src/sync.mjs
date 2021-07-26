import { sql } from './db/index.mjs'
import File from './lib/file.mjs'
import { getDirection } from './util.mjs'
import localCopy from './local/copy.mjs'
import localRemove from './local/remove.mjs'
import s3upload from './s3/upload.mjs'
import s3download from './s3/download.mjs'
import s3copy from './s3/copy.mjs'
import s3remove from './s3/remove.mjs'
import gdriveDownload from './drive/download.mjs'

export default async function sync (srcRoot, dstRoot, opts = {}) {
  srcRoot = File.fromUrl(srcRoot, { resolve: true, directory: true })
  dstRoot = File.fromUrl(dstRoot, { resolve: true, directory: true })
  const fn = getFunctions(srcRoot, dstRoot)

  await srcRoot.scan()
  await dstRoot.scan()

  const seen = new Set()

  // new files
  for (const row of fn.newFiles(fn.paths)) {
    const src = File.like(srcRoot, row)
    const dst = src.rebase(srcRoot, dstRoot)
    await fn.copy(src, dst, { ...opts, progress: true })
    seen.add(dst.path)
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
    seen.add(dst.path)
  }

  // complex renames
  for (const { contentId } of fn.duplicates()) {
    const d = { contentId, ...fn.paths }
    const srcs = fn.srcContent.all(d).map(row => File.like(srcRoot, row))
    const dsts = fn.dstContent.all(d).map(row => File.like(dstRoot, row))
    for (const src of srcs) {
      const dst = src.rebase(srcRoot, dstRoot)
      if (!dsts.find(d => d.url === dst.url)) {
        const cpy = dsts.find(d => !d.archived)
        if (cpy) {
          await fn.destCopy(cpy, dst, opts)
        } else {
          await fn.copy(src, dst, { ...opts, progress: true })
        }
        seen.add(dst.path)
      }
    }

    if (opts.delete) {
      for (const dst of dsts) {
        const src = dst.rebase(dstRoot, srcRoot)
        if (!srcs.find(s => s.url === src.url)) {
          await fn.remove(dst, opts)
          seen.add(dst.path)
        }
      }
    }
  }

  // deletes
  if (opts.delete) {
    for (const row of fn.oldFiles(fn.paths)) {
      const dst = File.like(dstRoot, row)
      if (!seen.has(dst.path)) {
        await fn.remove(dst, opts)
      }
    }
  }
}

function getFunctions (src, dst) {
  const dir = getDirection(src, dst)
  const paths = {
    localPath: (src.isLocal && src.path) || (dst.isLocal && dst.path),
    s3Bucket: (src.isS3 && src.bucket) || (dst.isS3 && dst.bucket),
    s3Path: (src.isS3 && src.path) || (dst.isS3 && dst.path),
    gdrivePath: (src.isGdrive && src.path) || (dst.isGdrive && dst.path)
  }
  return {
    local_s3: {
      paths,
      newFiles: localNotS3.all,
      oldFiles: s3NotLocal.all,
      differences: localS3Diff.all,
      duplicates: duplicates.all,
      srcContent: localContent,
      dstContent: s3Content,
      copy: s3upload,
      destCopy: s3copy,
      remove: s3remove
    },
    s3_local: {
      paths,
      newFiles: s3NotLocal.all,
      oldFiles: localNotS3.all,
      differences: localS3Diff.all,
      duplicates: duplicates.all,
      srcContent: s3Content,
      dstContent: localContent,
      copy: s3download,
      destCopy: localCopy,
      remove: localRemove
    },
    gdrive_local: {
      paths,
      newFiles: gdriveNotLocal.all,
      oldFiles: localNotGdrive.all,
      differences: localGdriveDiff.all,
      duplicates: duplicates.all,
      srcContent: gdriveContent,
      dstContent: localContent,
      copy: gdriveDownload,
      destCopy: localCopy,
      remove: localRemove
    }
  }[dir]
}

const localNotS3 = sql(`
  SELECT * FROM local_file_view
  WHERE path BETWEEN $localPath AND $localPath || '~'
  AND contentId NOT IN (
    SELECT contentId
    FROM s3_file
    WHERE bucket = $s3Bucket
    AND   path BETWEEN $s3Path AND $s3Path || '~'
  )
`)

const s3NotLocal = sql(`
  SELECT * FROM s3_file_view
  WHERE bucket = $s3Bucket
  AND   path BETWEEN $s3Path AND $s3Path || '~'
  AND   contentId NOT IN (
    SELECT contentId
    FROM local_file
    WHERE path BETWEEN $localPath AND $localPath || '~'
  )
`)

const localNotGdrive = sql(`
  SELECT * FROM local_file_view
  WHERE path BETWEEN $localPath AND $localPath || '~'
  AND contentId NOT IN (
    SELECT contentId
    FROM gdrive_file
    WHERE path BETWEEN $gdrivePath AND $gdrivePath || '~'
  )
`)

const gdriveNotLocal = sql(`
  SELECT * FROM gdrive_file_view
  WHERE path BETWEEN $gdrivePath AND $gdrivePath || '~'
  AND contentId NOT IN (
    SELECT contentId
    FROM local_file
    WHERE path BETWEEN $localPath AND $localPath || '~'
  )
`)

const localS3Diff = sql(`
  SELECT * FROM local_and_s3_view
  WHERE localPath BETWEEN $localPath AND $localPath || '~'
  AND   s3Bucket = $s3Bucket
  AND   s3Path BETWEEN $s3Path AND $s3Path || '~'
  AND   substr(localPath, 1 + length($localPath)) !=
          substr(s3Path, 1 + length($s3Path))
`)

const localGdriveDiff = sql(`
  SELECT * FROM local_and_gdrive_view
  WHERE localPath BETWEEN $localPath AND $localPath || '~'
  AND   gdrivePath BETWEEN $gdrivePath AND $gdrivePath || '~'
  AND   substr(localPath, 1 + length($localPath)) !=
          substr(gdrivePath, 1 + length($gdrivePath))
`)

const duplicates = sql(`
  SELECT contentId FROM duplicates_view
`)

const localContent = sql(`
  SELECT * FROM local_file_view
  WHERE contentId = $contentId
  AND   path BETWEEN $localPath AND $localPath || '~'
`)

const s3Content = sql(`
  SELECT * FROM s3_file_view
  WHERE contentId = $contentId
  AND   bucket = $s3Bucket
  AND   path BETWEEN $s3Path AND $s3Path || '~'
`)

const gdriveContent = sql(`
  SELECT * FROM gdrive_file_view
  WHERE contentId = $contentId
  AND   path BETWEEN $gdrivePath AND $gdrivePath || '~'
`)
