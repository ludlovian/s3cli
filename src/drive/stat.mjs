import { sql } from '../db/index.mjs'

export default async function stat (file) {
  const row = getDetails.get(file)
  if (!row) {
    throw new Error('Not found: ' + file.url)
  }
  file.googleId = row.googleId
  file.mtime = new Date(row.mtime + 'Z')
  file.size = row.size
  file.contentType = row.contentType
  file.md5Hash = row.md5Hash
}

const getDetails = sql(`
  SELECT *
  FROM gdrive_file_view
  WHERE path = $path
`)
