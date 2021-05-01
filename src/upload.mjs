import { upload as s3upload } from 's3js'

import report from './report.mjs'

export default function upload (file, url, { progress, limit }) {
  return s3upload(file, url, {
    onProgress: !!progress && doProgress(url),
    limit
  })
}

function doProgress (url) {
  report('file.transfer.start', url)
  const direction = 'uploaded'
  return data => {
    const { bytes, done, speedo } = data
    const { percent, total, taken, eta, rate: speed } = speedo
    const payload = { bytes, percent, total, taken, eta, speed, direction }
    report(`file.transfer.${done ? 'done' : 'update'}`, payload)
  }
}
