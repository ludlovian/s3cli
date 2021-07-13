import { sql } from '../db/index.mjs'

export const insertFile = sql(`
  INSERT INTO gdrive_file_view
    (path, size, mtime, googleId, contentType, md5Hash)
  VALUES
    ($path, $size, $mtime, $googleId, $contentType, $md5Hash)
`)

export const listFiles = sql(`
  SELECT  path
  FROM    gdrive_file
  WHERE   path LIKE $path || '%'
`)

export const removeFile = sql(`
  DELETE FROM gdrive_file
  WHERE path = $path
`)
