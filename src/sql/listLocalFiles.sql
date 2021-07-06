SELECT  path    AS path,
        size    AS size,
        mtime   AS mtime,
        contentType
                AS contentType,
        NULL    AS storage,
        md5Hash AS md5Hash

FROM    local_file_view

WHERE   path LIKE $path || '%'

ORDER BY path;
