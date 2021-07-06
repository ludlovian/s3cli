SELECT  bucket  AS bucket,
        path    AS path,
        size    AS size,
        mtime   AS mtime,
        contentType
                AS contentType,
        storage AS storage,
        md5Hash AS md5Hash

FROM    s3_file_view

WHERE   bucket  = $bucket
AND     path    LIKE $path || '%'

ORDER BY bucket,
         path;
