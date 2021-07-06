-- retrieves the md5Hash if known

-- params
--      path
--      mtime
--      size

SELECT  c.md5Hash

FROM    content c
JOIN    local_file f USING (contentId)

WHERE   f.path  = $path
AND     c.size  = $size
AND     f.mtime = datetime($mtime);
