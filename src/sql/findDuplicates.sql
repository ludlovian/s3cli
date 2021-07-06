WITH dups AS (
    SELECT  contentId
    FROM    local_file
    GROUP BY contentId
    HAVING  count(contentId) > 1
    UNION
    SELECT  contentId
    FROM    s3_file
    GROUP BY contentId
    HAVING  count(contentId) > 1
)
SELECT  contentId   AS contentId,
        's3://' || bucket || '/' || path
                    AS url
FROM    s3_file
WHERE   contentId IN (SELECT contentId FROM dups)

UNION ALL

SELECT  contentId   AS contentId,
        'file://' || path
                    AS url
FROM    local_file
WHERE   contentId IN (SELECT contentId FROM dups)
ORDER BY contentId;
