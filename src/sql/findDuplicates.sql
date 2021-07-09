-- all the content which appears more than once

SELECT  contentId
FROM    content_use_view
WHERE   remote_use > 1
OR      local_use > 1;
