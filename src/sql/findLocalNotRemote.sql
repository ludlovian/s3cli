-- Find files on local that are not on S3

-- params
--      localRoot
--      s3Root

SELECT  f.path      AS path,
        f.mtime     AS mtime,
        c.size      AS size,
        c.contentType
                    AS contentType,
        c.md5Hash   AS md5Hash

FROM    local_file       f
JOIN    content          c  USING (contentId)
JOIN    content_use_view u  USING (contentId)

WHERE   u.remote_use = 0
AND     u.local_use = 1
AND     'file://' || f.path
            LIKE $localRoot || '%'

ORDER BY f.path;
