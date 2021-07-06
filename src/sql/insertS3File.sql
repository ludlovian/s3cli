-- inserts a new S3 file into the database

-- params
--      bucket
--      path
--      md5Hash
--      size
--      contentType
--      mtime
--      storage

INSERT INTO content
    (md5Hash, size, contentType)
VALUES
    ($md5Hash, $size, $contentType)

ON CONFLICT DO UPDATE
SET contentType = excluded.contentType,
    updated     = excluded.updated;

INSERT INTO s3_file
    (bucket, path, contentId, mtime, storage)
SELECT  $bucket,
        $path,
        contentId,
        datetime($mtime),
        $storage
FROM    content
WHERE   md5Hash = $md5Hash
AND     size    = $size

ON CONFLICT DO UPDATE
    SET contentId   = excluded.contentId,
        mtime       = excluded.mtime,
        storage     = excluded.storage,
        updated     = excluded.updated;

