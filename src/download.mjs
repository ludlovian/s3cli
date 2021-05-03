import { resolve } from 'path'

import retry from 'retry'
import { download as s3download } from 's3js'

import report from './report.mjs'

export default function download (url, file, { progress, limit }) {
  const retryOpts = {
    retries: 5,
    delay: 5000,
    onRetry: data => report('retry', data)
  }
  const s3opts = { onProgress: !!progress && doProgress(url), limit }
  return retry(() => s3download(url, file, s3opts), retryOpts)
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
