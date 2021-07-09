-- find all the local files for this content
-- under the localroot

SELECT  'local'         AS type,
        f.path          AS path,
        f.mtime         AS mtime,
        c.size          AS size,
        c.md5Hash       AS md5Hash,
        c.contentType   AS contentType

FROM    local_file f
JOIN    content c USING (contentId)

WHERE   f.contentId = $contentId
AND     'file://' || f.path
            LIKE $localRoot || '%'

ORDER BY f.path;


