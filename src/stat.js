import { stat as s3stat } from 's3js'

import report from './report'

export default async function stat (url) {
  const data = await s3stat(url)
  const results = []
  for (let [k, v] of Object.entries(data)) {
    k = k.charAt(0).toLowerCase() + k.slice(1)
    if (k === 'metadata') continue
    if (k.endsWith('time')) v = new Date(v * 1000)
    if (k === 'mode') v = '0o' + v.toString(8)
    results.push([k, v])
  }
  results.sort((a, b) => (a[0] < b[b] ? -1 : a[0] > b[0] ? 1 : 0))
  const width = Math.max(...results.map(x => x[0].length))
  report('stat.start', url)
  for (const [key, value] of results) {
    report('stat.details', { key, value, width })
  }
  report('stat.done', url)
}
