CREATE TABLE IF NOT EXISTS hash (
    url    TEXT    NOT NULL PRIMARY KEY,
    mtime  TEXT    NOT NULL,
    "size" INTEGER NOT NULL,
    hash   TEXT
);

DROP TABLE IF EXISTS sync;

CREATE TEMP TABLE IF NOT EXISTS sync (
    "type" TEXT    NOT NULL,
    path   TEXT    NOT NULL,
    url    TEXT    NOT NULL,
    mtime  TEXT    NOT NULL,
    "size" INTEGER NOT NULL,
    PRIMARY KEY ("type", path)
);
