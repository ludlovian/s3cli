import { realpathSync } from 'fs'

import mime from 'mime'

import log from 'logjs'

import localScan from '../local/scan.mjs'
import localStat from '../local/stat.mjs'
import { parse as s3parse } from '../s3/util.mjs'
import s3scan from '../s3/scan.mjs'
import s3Stat from '../s3/stat.mjs'
import gdriveScan from '../drive/scan.mjs'
import gdriveStat from '../drive/stat.mjs'

export default class File {
  static fromUrl (url, opts = {}) {
    const { directory, resolve } = opts
    if (typeof url !== 'string') throw new Error('Not a string')
    if (url.startsWith('s3://')) {
      let { bucket, path } = s3parse(url)
      if (path) path = maybeAddSlash(path, directory)
      return new File({ type: 's3', bucket, path })
    } else if (url.startsWith('file://')) {
      let path = url.slice(7)
      if (resolve) path = realpathSync(path)
      path = maybeAddSlash(path, directory)
      return new File({ type: 'local', path })
    } else if (url.startsWith('gdrive://')) {
      let path = url.slice(9)
      path = maybeAddSlash(path, directory)
      return new File({ type: 'gdrive', path })
    } else if (url.includes('/')) {
      return File.fromUrl('file://' + url, opts)
    }
    throw new Error('Cannot understand ' + url)
  }

  static like ({ type }, data) {
    return new File({ type, ...data })
  }

  constructor (data) {
    this.type = data.type
    if (this.type === 's3') {
      this.bucket = data.bucket
      this.path = data.path
      this.storage = data.storage || 'STANDARD'
      this.metadata = data.metadata
    } else if (this.type === 'local') {
      this.path = data.path
    } else if (this.type === 'gdrive') {
      this.path = data.path
      this.googleId = data.googleId
    } else {
      throw new Error('Unkown type:' + data.type)
    }
    this.size = data.size
    this.mtime = data.mtime
    this.contentType = data.contentType
    this.md5Hash = undefined
    if (data.md5Hash) {
      if (!data.md5Hash.startsWith('"')) {
        this.md5Hash = data.md5Hash
      } else if (!data.md5Hash.includes('-')) {
        this.md5Hash = data.md5Hash.replaceAll('"', '')
      }
    }

    if (typeof this.mtime === 'string') {
      this.mtime = new Date(this.mtime + 'Z')
    }
    if (!this.contentType && !this.isDirectory) {
      this.contentType = mime.getType(this.path.split('.').pop())
    }
  }

  get isDirectory () {
    return this.path.endsWith('/')
  }

  get isS3 () {
    return this.type === 's3'
  }

  get isLocal () {
    return this.type === 'local'
  }

  get isGdrive () {
    return this.type === 'gdrive'
  }

  get hasStats () {
    return !!this.md5Hash
  }

  get archived () {
    return this.isS3 && !this.storage.toLowerCase().startsWith('standard')
  }

  get url () {
    if (this.isS3) {
      return `s3://${this.bucket}/${this.path}`
    } else if (this.isLocal) {
      return `file://${this.path}`
    } else {
      return `gdrive://${this.path}`
    }
  }

  async stat () {
    if (this.hasStats) return
    const fn = {
      s3: s3Stat,
      local: localStat,
      gdrive: gdriveStat
    }[this.type]

    if (fn) await fn(this)
  }

  rebase (from, to) {
    if (!this.url.startsWith(from.url)) {
      throw new Error(`${this.url} does not start with ${from.url}`)
    }
    return new File({
      ...this,
      type: to.type,
      bucket: to.bucket,
      path: to.path + this.path.slice(from.path.length)
    })
  }

  async scan () {
    let total
    log.status('Scanning %s ... ', this.url)

    const scanner = {
      local: localScan,
      s3: s3scan,
      gdrive: gdriveScan
    }[this.type]

    for await (const count of scanner(this)) {
      log.status('Scanning %s ... %d', this.url, count)
      total = count
    }
    log('%s files found on %s', total.toLocaleString(), this.url)
  }
}

function maybeAddSlash (str, addSlash) {
  if (addSlash) {
    if (str.endsWith('/')) return str
    return str + '/'
  } else {
    if (!str.endsWith('/')) return str
    return str.slice(0, -1)
  }
}
