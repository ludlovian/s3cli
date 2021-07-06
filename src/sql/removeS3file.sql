DELETE FROM s3_file
WHERE   bucket  = $bucket
AND     path    = $path;
