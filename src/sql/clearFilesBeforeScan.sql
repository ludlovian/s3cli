UPDATE  s3_file
SET     updated = NULL
WHERE   $url LIKE 's3://%'
AND     's3://' || bucket || '/' || path LIKE $url || '%';

UPDATE  local_file
SET     updated = NULL
WHERE   $url LIKE 'file://%'
AND     'file://' || path LIKE $url || '%';
