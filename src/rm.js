import { deleteObject } from 's3js'

import report from './report'

export default async function rm (url) {
  report('delete.file.start', url)
  await deleteObject(url)
  report('delete.file.done', url)
}
