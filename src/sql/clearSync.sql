
-- Clears out the sync table

-- Not normally necessary as this is a temporary table,
-- but in testing we run many syncs in the same exceution of
-- the program.

DELETE FROM sync
