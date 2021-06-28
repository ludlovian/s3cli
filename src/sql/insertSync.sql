
-- Records an actual file found during a sync

INSERT INTO sync
    (type, path, url, mtime, size)
VALUES
    ($type, $path, $url, datetime($mtime), $size)
