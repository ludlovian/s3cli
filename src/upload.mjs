import { upload as s3upload } from 's3js'
import retry from 'retry'

import report from './report.mjs'

export default function upload (file, url, { progress, limit }) {
  const retryOpts = {
    retries: 5,
    delay: 5000,
    onRetry: data => report('retry', data)
  }
  const s3opts = { onProgress: !!progress && doProgress(url), limit }
  return retry(() => s3upload(file, url, s3opts), retryOpts)
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
