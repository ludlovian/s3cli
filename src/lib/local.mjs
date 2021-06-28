import { EventEmitter } from 'events'
import {
  readdir,
  stat as fsStat,
  realpath,
  chmod,
  utimes,
  unlink,
  mkdir
} from 'fs/promises'
import {
  createReadStream as fsCreateReadStream,
  createWriteStream as fsCreateWriteStream
} from 'fs'
import { dirname, extname } from 'path'

import mime from 'mime'

import hashFile from 'hash-stream/simple'

import { selectHash, insertHash } from '../db/index.mjs'

export function list (baseurl) {
  const lister = new EventEmitter()
  let basepath = baseurl.slice(7)
  lister.done = (async () => {
    basepath = await realpath(basepath)
    if (!basepath.endsWith('/')) basepath += '/'
    await scan(basepath)
  })()
  return lister

  async function scan (dir) {
    const entries = await readdir(dir, { withFileTypes: true })
    const files = []
    const dirs = []
    for (const entry of entries) {
      const { name } = entry
      if (entry.isDirectory()) {
        dirs.push(dir + name + '/')
        continue
      }
      if (!entry.isFile()) continue
      const fullname = dir + name
      const path = fullname.slice(basepath.length)
      const url = 'file://' + fullname
      const stats = await fsStat(fullname)
      files.push({
        url,
        path,
        size: stats.size,
        mtime: new Date(Math.round(stats.mtimeMs)),
        mode: stats.mode & 0o777
      })
    }
    if (files.length) lister.emit('files', files)
    for (const dir of dirs) {
      await scan(dir)
    }
  }
}

export async function stat (url) {
  return await fsStat(url.slice(7))
}

export async function getHash (url, stats) {
  const path = url.slice(7)
  if (!stats) stats = await fsStat(path)
  const { mtime, size } = stats
  let hash = selectHash({ url, mtime, size })
  if (hash) return hash
  hash = await hashFile(path)
  insertHash({ url, mtime, size, hash })
  return hash
}

export async function createReadStream (url) {
  const path = url.slice(7)
  const stats = await fsStat(path)
  const hash = await getHash(url, stats)
  const attrs = {
    atime: stats.atime,
    ctime: stats.ctime,
    mtime: stats.mtime,
    uid: 1000,
    gid: 1000,
    uname: 'alan',
    gname: 'alan',
    mode: stats.mode & 0o777,
    md5: hash
  }
  const source = {
    size: stats.size,
    mtime: stats.mtime,
    contentType: mime.getType(extname(path)),
    hash,
    attrs
  }

  const stream = fsCreateReadStream(path)
  stream.source = source
  return stream
}

export async function createWriteStream (url, source) {
  const path = url.slice(7)
  await mkdir(dirname(path), { recursive: true })
  const { attrs } = source
  const mtime = attrs && attrs.mtime
  const mode = attrs && attrs.mode
  const stream = fsCreateWriteStream(path)
  const streamComplete = new Promise((resolve, reject) =>
    stream.on('error', reject).on('finish', resolve)
  )
  stream.done = streamComplete.then(async () => {
    if (mode) await chmod(path, mode)
    if (mtime) await utimes(path, mtime, mtime)
    return await getHash(url)
  })
  return stream
}

export async function remove (url) {
  const path = url.slice(7)
  await unlink(path)
}
