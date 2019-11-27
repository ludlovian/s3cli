'use strict'

import { scan } from 's3js'
import log from './log'
import { comma } from './util'

export default async function ls (url, options) {
  const { long, directory } = options

  if (directory && !url.endsWith('/')) url += '/'

  const opts = {
    Delimiter: directory ? '/' : undefined
  }

  for await (const data of scan(url, opts)) {
    const { Key, Prefix, ETag = '', Size } = data
    if (Key && Key.endsWith('/')) continue
    log(
      [
        long ? ETag.replace(/"/g, '').padEnd(33) : '',
        long ? `${comma(Size).padStart(13)}  ` : '',
        Prefix || Key
      ].join('')
    )
  }
}
