'use strict'

import { upload as s3upload } from 's3js'

import Speedo from './speedo'
import { time, comma, size } from './util'
import log from './log'

export default function upload (file, url, { progress, limit }) {
  return s3upload(file, url, {
    onProgress: progress ? doProgress(url) : undefined,
    limit
  })
}

function doProgress (url) {
  log(url)
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
          `${comma(bytes)} bytes uploaded `,
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
