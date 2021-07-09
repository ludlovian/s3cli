-- find all the remote files for this content
-- under the right root

SELECT  's3'            AS type,
        f.bucket        AS bucket,
        f.path          AS path,
        f.mtime         AS mtime,
        f.storage       AS storage,
        c.size          AS size,
        c.md5Hash       AS md5Hash,
        c.contentType   AS contentType

FROM    s3_file f
JOIN    content c USING (contentId)

WHERE   f.contentId = $contentId
AND     's3://' || f.bucket || '/' || f.path
            LIKE $s3Root || '%'

ORDER BY f.bucket, f.path;
