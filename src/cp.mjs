import retry from 'retry'

import { copy } from './vfs.mjs'
import { validateUrl } from './util.mjs'
import report from './report.mjs'

export default async function cp (fromUrl, toUrl, opts) {
  fromUrl = validateUrl(fromUrl)
  toUrl = validateUrl(toUrl)

  const { limit, progress, dryRun } = opts
  if (dryRun) return report('cp.dryrun', { url: toUrl })

  const copyOpts = {
    limit,
    onProgress: progress ? doProgress(toUrl) : undefined
  }
  const retryOpts = {
    retries: 5,
    delay: 5000,
    onRetry: data => report('retry', data)
  }

  await retry(() => copy(fromUrl, toUrl, copyOpts), retryOpts)
  if (!progress) {
    report('cp', { url: toUrl, ...opts })
  }
}

function doProgress (url) {
  report('cp.start', url)
  return data => {
    const { bytes, done, speedo } = data
    const { percent, total, taken, eta, rate: speed } = speedo
    const payload = { bytes, percent, total, eta, speed, taken }
    report(`cp.${done ? 'done' : 'update'}`, payload)
  }
}
