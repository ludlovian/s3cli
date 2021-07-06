-- inserts a new local file into the database

-- params
--      path
--      md5Hash
--      size
--      contentType
--      mtime

INSERT INTO content
    (md5Hash, size, contentType)
VALUES
    ($md5Hash, $size, $contentType)

ON CONFLICT DO UPDATE
SET contentType = excluded.contentType,
    updated     = excluded.updated;

INSERT INTO local_file
    (path, contentId, mtime)
SELECT  $path,
        contentId,
        datetime($mtime)
FROM    content
WHERE   md5Hash = $md5Hash
AND     size    = $size

ON CONFLICT DO UPDATE
    SET contentId   = excluded.contentId,
        mtime       = excluded.mtime,
        updated     = excluded.updated;
