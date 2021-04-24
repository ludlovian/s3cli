import { resolve } from 'path'
import { download as s3download } from 's3js'

import Speedo from 'speedo'

import report from './report.mjs'

export default function download (url, file, { progress, limit }) {
  return s3download(url, file, {
    onProgress: progress ? doProgress(file) : undefined,
    limit
  })
}

function doProgress (dest) {
  report('file.transfer.start', resolve(dest))
  const speedo = new Speedo()
  const direction = 'downloaded'
  return ({ bytes, total, done }) => {
    speedo.update({ current: bytes, total })
    report(`file.transfer.${done ? 'done' : 'update'}`, {
      bytes,
      percent: speedo.percent(),
      total,
      taken: speedo.taken(),
      eta: speedo.eta(),
      speed: speedo.rate(),
      direction
    })
  }
}
