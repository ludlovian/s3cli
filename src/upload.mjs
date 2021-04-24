import { upload as s3upload } from 's3js'

import Speedo from 'speedo'

import report from './report.mjs'

export default function upload (file, url, { progress, limit }) {
  return s3upload(file, url, {
    onProgress: progress ? doProgress(url) : undefined,
    limit
  })
}

function doProgress (url) {
  report('file.transfer.start', url)
  const speedo = new Speedo()
  const direction = 'uploaded'
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
