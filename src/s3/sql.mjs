import { sql } from '../db/index.mjs'

export const insertFile = sql(`
  INSERT INTO s3_file_view
    (bucket, path, size, mtime, storage, contentType, md5Hash)
  VALUES
    ($bucket, $path, $size, $mtime, $storage, $contentType, $md5Hash)
`)

export const markFilesOld = sql(`
  UPDATE s3_file
  SET    updated = NULL
  WHERE  bucket = $bucket
  AND    path LIKE $path || '%'
`)

export const cleanFiles = sql(`
  DELETE FROM s3_file
  WHERE  updated IS NULL
`)

export const removeFile = sql(`
  DELETE FROM s3_file
  WHERE bucket = $bucket
  AND   path = $path
`)
