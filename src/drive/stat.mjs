import db from '../db.mjs'

export default async function stat (file) {
  const row = db.getGDriveFiles(file)[0]
  if (!row) {
    throw new Error('Not found: ' + file.url)
  }
  file.googleId = row.googleId
  file.mtime = new Date(row.mtime + 'Z')
  file.size = row.size
  file.contentType = row.contentType
  file.md5Hash = row.md5Hash
}
