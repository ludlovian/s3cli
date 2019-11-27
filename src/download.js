'use strict'

import { resolve } from 'path'
import { download as s3download } from 's3js'

import log from './log'
import Speedo from './speedo'
import { time, comma, size } from './util'

export default function download (url, file, { progress, limit }) {
  return s3download(url, file, {
    onProgress: progress ? doProgress(file) : undefined,
    limit
  })
}

function doProgress (dest) {
  log(resolve(dest))
  const speedo = new Speedo()
  let started = false
  let sizeLength
  return ({ bytes, total, done }) => {
    if (!started) {
      started = true
      speedo.total = total
      sizeLength = comma(total).length
    }

    speedo.update(bytes)
    if (done) {
      const taken = speedo.taken()
      log(
        [
          '  ',
          `${comma(bytes)} bytes downloaded `,
          `in ${time(taken)} `,
          `at ${size(bytes / taken)}B/s`
        ].join('')
      )
    } else {
      log.status(
        [
          '  ',
          `${comma(bytes).padStart(sizeLength)} `,
          `${speedo
            .percent()
            .toString()
            .padStart(3)}% `,
          `time ${time(speedo.taken())} `,
          `eta ${time(speedo.eta())} `,
          `rate ${size(speedo.rate())}B/s`
        ].join('')
      )
    }
  }
}
