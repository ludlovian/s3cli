'use strict'

import EventEmitter from 'events'
import { relative } from 'path'

import { parseAddress as s3parse, scan as s3scan } from 's3js'

export default class Remote extends EventEmitter {
  constructor (data) {
    super()
    Object.assign(this, data)
  }

  static async * scan (root, filter) {
    const { Bucket, Key: Prefix } = s3parse(root)
    for await (const data of s3scan(root + '/')) {
      const path = relative(Prefix, data.Key)
      if (data.Key.endsWith('/') || !filter(path)) continue
      yield new Remote({
        path,
        root,
        url: `${Bucket}/${data.Key}`,
        hash: data.ETag.replace(/"/g, '')
      })
    }
  }
}
