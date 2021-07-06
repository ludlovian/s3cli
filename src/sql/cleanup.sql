-- cleans the database of old entries

DELETE FROM s3_file
WHERE updated IS NULL;

DELETE FROM local_file
WHERE updated IS NULL;

DELETE FROM content
WHERE contentId NOT IN (
    SELECT contentId FROM s3_file
    UNION
    SELECT contentId FROM local_file
);
