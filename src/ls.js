'use strict'

import { scan } from 's3js'
import log from './log'
import { comma, size } from './util'

export default async function ls (url, options) {
  const { long, directory, human, total } = options

  if (directory && !url.endsWith('/')) url += '/'

  const opts = {
    Delimiter: directory ? '/' : undefined
  }

  let numFiles = 0
  let totalBytes = 0

  for await (const data of scan(url, opts)) {
    const { Key, Prefix, ETag = '', Size } = data
    if (Key && Key.endsWith('/')) continue

    numFiles++
    totalBytes += Size

    const line = [Prefix || Key]
    if (long) {
      const md5 = ETag.replace(/"/g, '')
      const filesize = human ? size(Size) : Size.toString()
      line.splice(0, 0, md5.padEnd(32), filesize.padStart(10))
    }
    log(line.join(' '))
  }
  if (total) {
    const s = human ? `${size(totalBytes)}B` : `${comma(totalBytes)} bytes`
    log(`\n${s} in ${comma(numFiles)} file(s).`)
  }
}
