import { remove } from './vfs.mjs'
import { validateUrl } from './util.mjs'
import report from './report.mjs'

export default async function rm (url, opts) {
  url = validateUrl(url)
  const { dryRun } = opts
  if (dryRun) return report('rm.dryrun', url)
  report('rm', url)
  await remove(url)
}
