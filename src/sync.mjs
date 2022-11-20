import db from './db.mjs'
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
  db.open()
  srcRoot = File.fromUrl(srcRoot, { resolve: true, directory: true })
  dstRoot = File.fromUrl(dstRoot, { resolve: true, directory: true })
  const fn = getFunctions(srcRoot, dstRoot)

  await srcRoot.scan()
  await dstRoot.scan()

  const seen = new Set()

  // new files
  for (const row of fn.newFiles()) {
    const src = File.like(srcRoot, row)
    const dst = src.rebase(srcRoot, dstRoot)
    await fn.copy(src, dst, { ...opts, progress: true })
    seen.add(dst.path)
  }

  // simple renames
  for (const { contentId } of fn.differences()) {
    const src = File.like(srcRoot, fn.srcContent(contentId)[0])
    const dst = src.rebase(srcRoot, dstRoot)
    const old = File.like(dstRoot, fn.dstContent(contentId)[0])
    if (!old.archived) {
      await fn.destCopy(old, dst, opts)
    } else {
      await fn.copy(src, dst, { ...opts, progress: true })
    }
    seen.add(dst.path)
  }

  // complex renames
  for (const contentId of fn.duplicates()) {
    const srcs = fn.srcContent(contentId).map(row => File.like(srcRoot, row))
    const dsts = fn.dstContent(contentId).map(row => File.like(dstRoot, row))
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
    for (const row of fn.oldFiles()) {
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
      newFiles: () => db.getLocalNotS3(paths),
      oldFiles: () => db.getS3NotLocal(paths),
      differences: () => db.getLocalS3Diffs(paths),
      duplicates: () => db.getDuplicates(),
      srcContent: contentId => db.getLocalContent({ ...paths, contentId }),
      dstContent: contentId => db.getS3Content({ ...paths, contentId }),
      copy: s3upload,
      destCopy: s3copy,
      remove: s3remove
    },
    s3_local: {
      newFiles: () => db.getS3NotLocal(paths),
      oldFiles: () => db.getLocalNotS3(paths),
      differences: () => db.getLocalS3Diffs(paths),
      duplicates: () => db.getDuplicates(),
      srcContent: contentId => db.getS3Content({ ...paths, contentId }),
      dstContent: contentId => db.getLocalContent({ ...paths, contentId }),
      copy: s3download,
      destCopy: localCopy,
      remove: localRemove
    },
    gdrive_local: {
      newFiles: () => db.getGdriveNotLocal(paths),
      oldFiles: () => db.getLocalNotGdrive(paths),
      differences: () => db.getLocalGdriveDiffs(paths),
      duplicates: () => db.getDuplicates(),
      srcContent: contentId => db.getGdriveContent({ ...paths, contentId }),
      dstContent: contentId => db.getLocalContent({ ...paths, contentId }),
      copy: gdriveDownload,
      destCopy: localCopy,
      remove: localRemove
    }
  }[dir]
}
