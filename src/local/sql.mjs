import { sql } from '../db/index.mjs'

export const listFiles = sql(`
  SELECT path
  FROM   local_file
  WHERE  path like $path || '%'
`)

export const insertFile = sql(`
  INSERT INTO local_file_view
    (path, size, mtime, contentType, md5Hash)
  VALUES
    ($path, $size, $mtime, $contentType, $md5Hash)
`)

export const removeFile = sql(`
  DELETE FROM local_file
  WHERE path = $path
`)

export const findHash = sql(`
  SELECT md5Hash
  FROM   local_file_view
  WHERE  path = $path
  AND    size = $size
  AND    mtime = datetime($mtime)
`).pluck().get
