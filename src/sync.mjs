import { sql } from './db/index.mjs'
import File from './lib/file.mjs'
import { getDirection } from './util.mjs'
import localCopy from './local/copy.mjs'
import localRemove from './local/remove.mjs'
import s3upload from './s3/upload.mjs'
import s3download from './s3/download.mjs'
import s3copy from './s3/copy.mjs'
import s3remove from './s3/remove.mjs'

export default async function sync (srcRoot, dstRoot, opts = {}) {
  srcRoot = File.fromUrl(srcRoot, { resolve: true, directory: true })
  dstRoot = File.fromUrl(dstRoot, { resolve: true, directory: true })
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
        const cpy = dsts.find(d => !d.archived)
        if (cpy) {
          await fn.destCopy(cpy, dst, opts)
        } else {
          await fn.copy(src, dst, { ...opts, progress: true })
        }
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

function getFunctions (src, dst) {
  const dir = getDirection(src, dst)
  const paths = {
    localPath: (src.isLocal && src.path) || (dst.isLocal && dst.path),
    s3Bucket: (src.isS3 && src.bucket) || (dst.isS3 && dst.bucket),
    s3Path: (src.isS3 && src.path) || (dst.isS3 && dst.path)
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
    }
  }[dir]
}

const localNotS3 = sql(`
  SELECT * FROM local_file_view
  WHERE path LIKE $localPath || '%'
  AND contentId NOT IN (
    SELECT contentId
    FROM s3_file
    WHERE bucket = $s3Bucket
    AND   path LIKE $s3Path || '%'
  )
`)

const s3NotLocal = sql(`
  SELECT * FROM s3_file_view
  WHERE bucket = $s3Bucket
  AND   path LIKE $s3Path || '%'
  AND   contentId NOT IN (
    SELECT contentId
    FROM local_file
    WHERE path LIKE $localPath || '%'
  )
`)

const localS3Diff = sql(`
  SELECT * FROM local_and_s3_view
  WHERE localPath LIKE $localPath || '%'
  AND   s3Bucket = $s3Bucket
  AND   s3Path LIKE $s3Path || '%'
  AND   substr(localPath, 1 + length($localPath)) !=
          substr(s3Path, 1 + length($s3Path))
`)

const duplicates = sql(`
  SELECT contentId FROM duplicates_view
`)

const localContent = sql(`
  SELECT * FROM local_file_view
  WHERE contentId = $contentId
  AND   path LIKE $localPath || '%'
`)

const s3Content = sql(`
  SELECT * FROM s3_file_view
  WHERE contentId = $contentId
  AND   bucket = $s3Bucket
  AND   path LIKE $s3Path || '%'
`)
