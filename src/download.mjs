import { resolve } from 'path'
import { download as s3download } from 's3js'

import report from './report.mjs'

export default function download (url, file, { progress, limit }) {
  return s3download(url, file, {
    onProgress: !!progress && doProgress(file),
    limit
  })
}

function doProgress (dest) {
  report('file.transfer.start', resolve(dest))
  const direction = 'downloaded'
  return data => {
    const { bytes, done, speedo } = data
    const { total, percent, eta, taken, rate: speed } = speedo
    const payload = { bytes, percent, total, taken, eta, speed, direction }
    report(`file.transfer.${done ? 'done' : 'update'}`, payload)
  }
}
