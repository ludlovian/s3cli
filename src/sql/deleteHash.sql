
-- Removes the stored hash for a given file

DELETE FROM hash

WHERE url = $url
