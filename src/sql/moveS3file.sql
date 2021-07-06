UPDATE s3_file

SET     path   = $newPath

WHERE   bucket = $bucket
AND     path   = $oldPath;

