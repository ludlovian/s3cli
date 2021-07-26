import { sql } from '../db/index.mjs'

export const insertFile = sql(`
  INSERT INTO s3_file_view
    (bucket, path, size, mtime, storage, contentType, md5Hash)
  VALUES
    ($bucket, $path, $size, $mtime, $storage, $contentType, $md5Hash)
`)

export const listFiles = sql(`
  SELECT  bucket, path
  FROM    s3_file
  WHERE   bucket = $bucket
  AND     path BETWEEN $path AND $path || '~'
`)

export const removeFile = sql(`
  DELETE FROM s3_file
  WHERE bucket = $bucket
  AND   path = $path
`)
