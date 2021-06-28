
-- Counts how many source files we found in the sync

SELECT count(*)

FROM sync

WHERE type = 'src'
