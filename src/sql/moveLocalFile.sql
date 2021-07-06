UPDATE  local_file
SET     path = $newPath
WHERE   path = $oldPath;
