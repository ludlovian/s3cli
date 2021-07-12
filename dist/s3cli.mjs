#!/usr/bin/env node
import sade from 'sade';
import { format } from 'util';
import { createReadStream, realpathSync, createWriteStream } from 'fs';
import mime from 'mime';
import { readdir, lstat, mkdir, utimes, copyFile, unlink, rmdir } from 'fs/promises';
import { resolve, join, dirname } from 'path';
import { homedir } from 'os';
import SQLite from 'better-sqlite3';
import { createHash } from 'crypto';
import AWS from 'aws-sdk';
import { PassThrough } from 'stream';
import { pipeline } from 'stream/promises';

const allColours = (
  '20,21,26,27,32,33,38,39,40,41,42,43,44,45,56,57,62,63,68,69,74,75,76,' +
  '77,78,79,80,81,92,93,98,99,112,113,128,129,134,135,148,149,160,161,' +
  '162,163,164,165,166,167,168,169,170,171,172,173,178,179,184,185,196,' +
  '197,198,199,200,201,202,203,204,205,206,207,208,209,214,215,220,221'
)
  .split(',')
  .map(x => parseInt(x, 10));

const painters = [];

function makePainter (n) {
  const CSI = '\x1b[';
  const set = CSI + (n < 8 ? n + 30 + ';22' : '38;5;' + n + ';1') + 'm';
  const reset = CSI + '39;22m';
  return s => {
    if (!s.includes(CSI)) return set + s + reset
    return removeExcess(set + s.replaceAll(reset, reset + set) + reset)
  }
}

function painter (n) {
  if (painters[n]) return painters[n]
  painters[n] = makePainter(n);
  return painters[n]
}

// eslint-disable-next-line no-control-regex
const rgxDecolour = /(^|[^\x1b]*)((?:\x1b\[[0-9;]+m)|$)/g;
function truncate (string, max) {
  max -= 2; // leave two chars at end
  if (string.length <= max) return string
  const parts = [];
  let w = 0;
  for (const [, txt, clr] of string.matchAll(rgxDecolour)) {
    parts.push(txt.slice(0, max - w), clr);
    w = Math.min(w + txt.length, max);
  }
  return removeExcess(parts.join(''))
}

// eslint-disable-next-line no-control-regex
const rgxSerialColours = /(?:\x1b\[[0-9;]+m)+(\x1b\[[0-9;]+m)/g;
function removeExcess (string) {
  return string.replaceAll(rgxSerialColours, '$1')
}

function randomColour () {
  const n = Math.floor(Math.random() * allColours.length);
  return allColours[n]
}

const colours = {
  black: 0,
  red: 1,
  green: 2,
  yellow: 3,
  blue: 4,
  magenta: 5,
  cyan: 6,
  white: 7
};

const CLEAR_LINE = '\r\x1b[0K';

const state = {
  dirty: false,
  width: process.stdout && process.stdout.columns,
  /* c8 ignore next */
  level: process.env.LOGLEVEL ? parseInt(process.env.LOGLEVEL, 10) : undefined,
  write: process.stdout.write.bind(process.stdout)
};

process.stdout &&
  process.stdout.on('resize', () => (state.width = process.stdout.columns));

function _log (
  args,
  { newline = true, limitWidth, prefix = '', level, colour }
) {
  if (level && (!state.level || state.level < level)) return
  const msg = format(...args);
  let string = prefix + msg;
  if (colour != null) string = painter(colour)(string);
  if (limitWidth) string = truncate(string, state.width);
  if (newline) string = string + '\n';
  if (state.dirty) string = CLEAR_LINE + string;
  state.dirty = !newline && !!msg;
  state.write(string);
}

function makeLogger (base, changes = {}) {
  const baseOptions = base ? base._preset : {};
  const options = {
    ...baseOptions,
    ...changes,
    prefix: (baseOptions.prefix || '') + (changes.prefix || '')
  };
  const configurable = true;
  const fn = (...args) => _log(args, options);
  const addLevel = level => makeLogger(fn, { level });
  const addColour = c =>
    makeLogger(fn, { colour: c in colours ? colours[c] : randomColour() });
  const addPrefix = prefix => makeLogger(fn, { prefix });
  const status = () => makeLogger(fn, { newline: false, limitWidth: true });

  const colourFuncs = Object.fromEntries(
    Object.entries(colours).map(([name, n]) => [
      name,
      { value: painter(n), configurable }
    ])
  );

  return Object.defineProperties(fn, {
    _preset: { value: options, configurable },
    _state: { value: state, configurable },
    name: { value: 'log', configurable },
    level: { value: addLevel, configurable },
    colour: { value: addColour, configurable },
    prefix: { value: addPrefix, configurable },
    status: { get: status, configurable },
    ...colourFuncs
  })
}

const log = makeLogger();

function once (fn) {
  function f (...args) {
    if (f.called) return f.value
    f.value = fn(...args);
    f.called = true;
    return f.value
  }

  if (fn.name) {
    Object.defineProperty(f, 'name', { value: fn.name, configurable: true });
  }

  return f
}

function tidy (sql) {
  return sql
    .split('\n')
    .map(line => line.trim())
    .filter(line => !line.startsWith('--'))
    .map(line => line.replaceAll(/  +/g, ' '))
    .filter(Boolean)
    .join(' ')
    .trim()
    .replaceAll(/; */g, ';')
    .replace(/;$/, '')
}

function statement (stmt, opts = {}) {
  const { pluck, all, get, db } = opts;
  function exec (...args) {
    args = args.map(arg => {
      if (!arg || typeof arg !== 'object') return arg
      return Object.fromEntries(
        Object.entries(arg).map(([k, v]) => [
          k,
          v instanceof Date ? v.toISOString() : v
        ])
      )
    });
    if (stmt.includes(';')) return db().exec(stmt)
    let prep = prepare(stmt, db);
    if (pluck) prep = prep.pluck();
    if (all) return prep.all(...args)
    if (get) return prep.get(...args)
    return prep.run(...args)
  }
  return Object.defineProperties(exec, {
    pluck: { value: () => statement(stmt, { ...opts, pluck: true }) },
    get: { get: () => statement(stmt, { ...opts, get: true }) },
    all: { get: () => statement(stmt, { ...opts, all: true }) }
  })
}

function transaction (_fn, db) {
  let fn;
  return (...args) => {
    if (!fn) fn = db().transaction(_fn);
    return fn(...args)
  }
}

const cache = new Map();
function prepare (stmt, db) {
  let p = cache.get(stmt);
  if (p) return p
  p = db().prepare(stmt);
  cache.set(stmt, p);
  return p
}

const DBVERSION = 4;

const db = once(() => {
  const dbFile =
    process.env.DB || resolve(homedir(), '.databases', 'files4.sqlite');
  const db = new SQLite(dbFile);
  db.exec(ddl);
  const version = db
    .prepare('select version from dbversion')
    .pluck()
    .get();
  if (version !== DBVERSION) {
    throw new Error('Wrong version of database: ' + dbFile)
  }
  return db
});

function sql (text) {
  return statement(tidy(text), { db })
}
sql.transaction = fn => transaction(fn, db);

const ddl = tidy(`
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

-- MAIN database ---------------------------------

-- Version of the database schema we are using

CREATE VIEW IF NOT EXISTS dbVersion AS
SELECT 4 AS version;

-- Unique item of content, as identified by (md5, size)
--

CREATE TABLE IF NOT EXISTS content(
    contentId   INTEGER PRIMARY KEY NOT NULL,
    md5Hash     TEXT NOT NULL,
    size        INTEGER NOT NULL,
    contentType TEXT,
    updated     TEXT DEFAULT (datetime('now')),
    UNIQUE (md5Hash, size)
);

-- A local file containing some content

CREATE TABLE IF NOT EXISTS local_file(
    path        TEXT NOT NULL PRIMARY KEY,
    contentId   INTEGER NOT NULL REFERENCES content(contentId),
    mtime       TEXT NOT NULL,
    updated     TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS local_file_i1
  ON local_file (contentId);

CREATE TRIGGER IF NOT EXISTS local_file_td
AFTER DELETE ON local_file
BEGIN
    DELETE FROM content
    WHERE   contentId = OLD.contentId
    AND     NOT EXISTS (
                SELECT contentId
                FROM   local_file
                WHERE  contentId = OLD.contentId)
    AND     NOT EXISTS (
                SELECT contentId
                FROM   s3_file
                WHERE  contentId = OLD.contentId);
END;

-- A file held on S3 with some content

CREATE TABLE IF NOT EXISTS s3_file(
    bucket      TEXT NOT NULL,
    path        TEXT NOT NULL,
    contentId   INTEGER NOT NULL REFERENCES content(contentId),
    mtime       TEXT NOT NULL,
    storage     TEXT NOT NULL,
    updated     TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (bucket, path)
);

CREATE INDEX IF NOT EXISTS s3_file_i1
  ON s3_file (contentId);

CREATE TRIGGER IF NOT EXISTS s3_file_td
AFTER DELETE ON s3_file
BEGIN
    DELETE FROM content
    WHERE   contentId = OLD.contentId
    AND     NOT EXISTS (
                SELECT contentId
                FROM   local_file
                WHERE  contentId = OLD.contentId)
    AND     NOT EXISTS (
                SELECT contentId
                FROM   s3_file
                WHERE  contentId = OLD.contentId);
END;

-- VIEWS ----------------------------------------

-- locally held file

CREATE VIEW IF NOT EXISTS local_file_view AS
SELECT  f.path          AS path,
        c.size          AS size,
        f.mtime         AS mtime,
        c.contentType   AS contentType,
        c.md5Hash       AS md5Hash,
        f.contentId     AS contentId
FROM    local_file f
JOIN    content c USING (contentId)
ORDER BY f.path;

CREATE TRIGGER IF NOT EXISTS local_file_view_ti
INSTEAD OF INSERT ON local_file_view
BEGIN
    INSERT INTO content
        (md5Hash, size, contentType)
    VALUES
        (NEW.md5Hash, NEW.size, NEW.contentType)
    ON CONFLICT DO UPDATE
        SET contentType = excluded.contentType,
            updated     = excluded.updated
        WHERE contentType != excluded.contentType;

    INSERT INTO local_file
        (path, contentId, mtime)
    SELECT  NEW.path,
            contentId,
            datetime(NEW.mtime)
    FROM    content
    WHERE   md5Hash = NEW.md5Hash
    AND     size    = NEW.size

    ON CONFLICT DO UPDATE
        SET contentId   = excluded.contentId,
            mtime       = excluded.mtime,
            updated     = excluded.updated
        WHERE contentId != excluded.contentId
        OR    mtime     != excluded.mtime;
END;

-- S3 file

CREATE VIEW IF NOT EXISTS s3_file_view AS
SELECT  f.bucket        AS bucket,
        f.path          AS path,
        c.size          AS size,
        f.mtime         AS mtime,
        f.storage       AS storage,
        c.contentType   AS contentType,
        c.md5Hash       AS md5Hash,
        f.contentId     AS contentId
FROM    s3_file f
JOIN    content c USING (contentId)
ORDER BY f.bucket, f.path;

CREATE TRIGGER IF NOT EXISTS s3_file_view_ti
INSTEAD OF INSERT ON s3_file_view
BEGIN
    INSERT INTO content
        (md5Hash, size, contentType)
    VALUES
        (NEW.md5Hash, NEW.size, NEW.contentType)
    ON CONFLICT DO UPDATE
        SET contentType = excluded.contentType,
            updated     = excluded.updated
        WHERE contentType != excluded.contentType;

    INSERT INTO s3_file
        (bucket, path, contentId, mtime, storage)
    SELECT  NEW.bucket,
            NEW.path,
            contentId,
            datetime(NEW.mtime),
            NEW.storage
    FROM    content
    WHERE   md5Hash = NEW.md5Hash
    AND     size    = NEW.size

    ON CONFLICT DO UPDATE
        SET contentId   = excluded.contentId,
            mtime       = excluded.mtime,
            storage     = excluded.storage,
            updated     = excluded.updated
        WHERE contentId != excluded.contentId
        OR    mtime     != excluded.mtime
        OR    storage   != excluded.storage;
END;

-- On local but not on S3 --

CREATE VIEW IF NOT EXISTS local_not_s3_view AS
SELECT *
FROM   local_file_view
WHERE  contentId NOT IN (
  SELECT contentId
  FROM   s3_file);

-- On S3 but not on local --

CREATE VIEW IF NOT EXISTS s3_not_local_view AS
SELECT *
FROM   s3_file_view
WHERE  contentId NOT IN (
  SELECT contentId
  FROM   local_file);

-- On local and S3

CREATE VIEW IF NOT EXISTS local_and_s3_view AS
SELECT l.path       AS localPath,
       s.bucket     AS s3Bucket,
       s.path       AS s3Path,
       l.contentId  as contentId
FROM   local_file l
JOIN   s3_file s USING (contentId)
GROUP BY contentId
HAVING count(l.path) = 1
AND    count(s.path) = 1;

-- Multiple copies exist somewhere

CREATE VIEW IF NOT EXISTS duplicates_view AS
SELECT  contentId AS contentId
FROM    local_file
GROUP BY contentId
HAVING count(contentId) > 1

UNION

SELECT  contentId AS contentId
FROM    s3_file
GROUP BY contentId
HAVING count(contentId) > 1;

COMMIT;
`);

const listFiles$1 = sql(`
  SELECT path
  FROM   local_file
  WHERE  path like $path || '%'
`);

const insertFile$1 = sql(`
  INSERT INTO local_file_view
    (path, size, mtime, contentType, md5Hash)
  VALUES
    ($path, $size, $mtime, $contentType, $md5Hash)
`);

const removeFile$1 = sql(`
  DELETE FROM local_file
  WHERE path = $path
`);

const findHash = sql(`
  SELECT md5Hash
  FROM   local_file_view
  WHERE  path = $path
  AND    size = $size
  AND    mtime = datetime($mtime)
`).pluck().get;

async function * scan$1 (root) {
  const File = root.constructor;

  let n = 0;
  const old = new Set(listFiles$1.pluck().all(root));

  const insertFiles = sql.transaction(files => {
    for (const file of files) {
      insertFile$1(file);
      old.delete(file.path);
    }
  });
  const deleteOld = sql.transaction(paths => {
    for (const path of paths) {
      removeFile$1({ path });
    }
  });

  for await (const files of scanDir(root.path, File)) {
    n += files.length;
    insertFiles(files);
    yield n;
  }
  deleteOld([...old]);
}

async function * scanDir (dir, File) {
  const entries = await readdir(dir, { withFileTypes: true });
  const files = [];
  const dirs = [];
  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) dirs.push(path);
    if (!entry.isFile()) continue
    const file = new File({ type: 'local', path });
    await file.stat();
    files.push(file);
  }
  if (files.length) yield files;
  for (const dir of dirs) {
    yield * scanDir(dir, File);
  }
}

async function hashFile (filename, { algo = 'md5', enc = 'hex' } = {}) {
  const hasher = createHash(algo);
  for await (const chunk of createReadStream(filename)) {
    hasher.update(chunk);
  }
  return hasher.digest(enc)
}

async function stat$1 (file) {
  const stats = await lstat(file.path);
  file.size = stats.size;
  file.mtime = stats.mtime;
  file.md5Hash = findHash(file);
  if (!file.md5Hash) {
    log.status('%s ... hashing', file.path);
    file.md5Hash = await hashFile(file.path);
    insertFile$1(file);
  }
}

function getDirection (src, dst) {
  const validDirections = new Set(['local_s3', 's3_local']);
  const dir = src.type + '_' + dst.type;
  if (!validDirections.has(dir)) {
    throw new Error(`Cannot do ${src.type} -> ${dst.type}`)
  }
  return dir
}

function comma (n) {
  if (typeof n !== 'number') return n
  return n.toLocaleString()
}

function fmtTime (t) {
  t = Math.round(t / 1000);
  if (t < 60) return t + 's'
  t = Math.round(t / 60);
  return t + 'm'
}

function fmtSize (n) {
  const suffixes = [
    ['G', 1024 * 1024 * 1024],
    ['M', 1024 * 1024],
    ['K', 1024],
    ['', 1]
  ];

  for (const [suffix, factor] of suffixes) {
    if (n >= factor) return (n / factor).toFixed(1) + suffix
  }
  return '0'
}

const getS3 = once(() => {
  const REGION = 'eu-west-1';
  return new AWS.S3({ region: REGION })
});

function parse (url) {
  const m = /^s3:\/\/([^/]+)(?:\/(.*))?$/.exec(url);
  if (!m) throw new TypeError(`Bad S3 URL: ${url}`)
  return { bucket: m[1], path: m[2] || '' }
}

function onProgress ({ speedo }) {
  const { done, current, percent, total, taken, eta, rate } = speedo;
  if (!done) {
    const s = [
      comma(current).padStart(1 + comma(total).length),
      `${percent.toString().padStart(3)}%`,
      `time ${fmtTime(taken)}`,
      `eta ${fmtTime(eta)}`,
      `rate ${fmtSize(rate)}B/s`
    ].join(' ');
    log.status(s);
  } else {
    const s = [
      ` ${comma(total)} bytes copied`,
      `in ${fmtTime(taken)}`,
      `at ${fmtSize(rate)}B/s`
    ].join(' ');
    log(log.green(s));
  }
}

const insertFile = sql(`
  INSERT INTO s3_file_view
    (bucket, path, size, mtime, storage, contentType, md5Hash)
  VALUES
    ($bucket, $path, $size, $mtime, $storage, $contentType, $md5Hash)
`);

const listFiles = sql(`
  SELECT  bucket, path
  FROM    s3_file
  WHERE   bucket = $bucket
  AND     path LIKE $path || '%'
`);

const removeFile = sql(`
  DELETE FROM s3_file
  WHERE bucket = $bucket
  AND   path = $path
`);

async function * scan (root) {
  const File = root.constructor;
  let n = 0;
  const s3 = getS3();

  const old = new Set(listFiles.all(root).map(r => r.path));

  const insertFiles = sql.transaction(files => {
    for (const file of files) {
      insertFile(file);
      old.delete(file.path);
    }
  });

  const deleteOld = sql.transaction(paths => {
    const { bucket } = root;
    for (const path of paths) {
      removeFile({ bucket, path });
    }
  });

  const request = { Bucket: root.bucket, Prefix: root.path };
  while (true) {
    const result = await s3.listObjectsV2(request).promise();
    const files = [];
    for (const item of result.Contents) {
      const file = new File({
        type: 's3',
        bucket: root.bucket,
        path: item.Key,
        size: item.Size,
        mtime: item.LastModified,
        storage: item.StorageClass || 'STANDARD',
        md5Hash: item.ETag
      });
      if (!file.hasStats) await file.stat();
      files.push(file);
    }
    n += files.length;
    insertFiles(files);
    yield n;

    if (!result.IsTruncated) break
    request.ContinuationToken = result.NextContinuationToken;
  }

  deleteOld([...old]);
}

const MD_KEY = 's3cmd-attrs';

async function stat (file) {
  const s3 = getS3();
  const req = { Bucket: file.bucket, Key: file.path };
  const item = await s3.headObject(req).promise();
  file.mtime = item.LastModified;
  file.size = item.ContentLength;
  file.storage = item.StorageClass || 'STANDARD';
  if (item.Metadata && item.Metadata[MD_KEY]) {
    file.metadata = unpack(item.Metadata[MD_KEY]);
  }
  if (!item.ETag.includes('-')) {
    file.md5Hash = item.ETag.replaceAll('"', '');
  } else if (file.metadata && file.metadata.md5) {
    file.md5Hash = file.metadata.md5;
  } else {
    throw new Error('Could not get md5 hash for ' + file.url)
  }
}

function unpack (s) {
  return Object.fromEntries(
    s
      .split('/')
      .map(x => x.split(':'))
      .map(([k, v]) => {
        if (!isNaN(Number(v))) v = Number(v);
        if (k.endsWith('time')) v = new Date(v);
        return [k, v]
      })
  )
}

class File {
  static fromUrl (url, opts = {}) {
    const { directory, resolve } = opts;
    if (typeof url !== 'string') throw new Error('Not a string')
    if (url.startsWith('s3://')) {
      let { bucket, path } = parse(url);
      if (path) path = maybeAddSlash(path, directory);
      return new File({ type: 's3', bucket, path })
    } else if (url.startsWith('file://')) {
      let path = url.slice(7);
      if (resolve) path = realpathSync(path);
      path = maybeAddSlash(path, directory);
      return new File({ type: 'local', path })
    } else if (url.includes('/')) {
      return File.fromUrl('file://' + url, opts)
    }
    throw new Error('Cannot understand ' + url)
  }

  static like ({ type }, data) {
    return new File({ type, ...data })
  }

  constructor (data) {
    this.type = data.type;
    if (this.type === 's3') {
      this.bucket = data.bucket;
      this.path = data.path;
      this.storage = data.storage || 'STANDARD';
      this.metadata = data.metadata;
    } else if (this.type === 'local') {
      this.path = data.path;
    } else {
      throw new Error('Unkown type:' + data.type)
    }
    this.size = data.size;
    this.mtime = data.mtime;
    this.contentType = data.contentType;
    this.md5Hash = undefined;
    if (data.md5Hash) {
      if (!data.md5Hash.startsWith('"')) {
        this.md5Hash = data.md5Hash;
      } else if (!data.md5Hash.includes('-')) {
        this.md5Hash = data.md5Hash.replaceAll('"', '');
      }
    }

    if (typeof this.mtime === 'string') {
      this.mtime = new Date(this.mtime + 'Z');
    }
    if (!this.contentType && !this.isDirectory) {
      this.contentType = mime.getType(this.path.split('.').pop());
    }
  }

  get isDirectory () {
    return this.path.endsWith('/')
  }

  get isS3 () {
    return this.type === 's3'
  }

  get isLocal () {
    return this.type === 'local'
  }

  get hasStats () {
    return !!this.md5Hash
  }

  get archived () {
    return this.isS3 && !this.storage.toLowerCase().startsWith('standard')
  }

  get url () {
    if (this.isS3) {
      return `s3://${this.bucket}/${this.path}`
    } else {
      return `file://${this.path}`
    }
  }

  async stat () {
    if (this.hasStats) return
    if (this.isS3) {
      await stat(this);
    } else {
      await stat$1(this);
    }
  }

  rebase (from, to) {
    if (!this.url.startsWith(from.url)) {
      throw new Error(`${this.url} does not start with ${from.url}`)
    }
    return new File({
      ...this,
      type: to.type,
      bucket: to.bucket,
      path: to.path + this.path.slice(from.path.length)
    })
  }

  async scan () {
    let total;
    log.status('Scanning %s ... ', this.url);

    const scanner = {
      local: scan$1,
      s3: scan
    }[this.type];

    for await (const count of scanner(this)) {
      log.status('Scanning %s ... %d', this.url, count);
      total = count;
    }
    log('%s files found on %s', total.toLocaleString(), this.url);
  }
}

function maybeAddSlash (str, addSlash) {
  if (addSlash) {
    if (str.endsWith('/')) return str
    return str + '/'
  } else {
    if (!str.endsWith('/')) return str
    return str.slice(0, -1)
  }
}

async function ls (url, opts) {
  const { long, rescan, human, total } = opts;
  url = File.fromUrl(url, { resolve: true });

  const list = {
    local: localList.all,
    s3: s3List.all
  }[url.type];

  if (rescan) await url.scan();

  let nTotalCount = 0;
  let nTotalSize = 0;

  for (const row of list(url)) {
    const { path, mtime, size, storage } = row;
    nTotalCount++;
    nTotalSize += size;
    let s = '';
    if (long) {
      s = (STORAGE[storage] || 'F') + '  ';
      const sz = human ? fmtSize(size) : size.toString();
      s += sz.padStart(10) + '  ';
      s += mtime + '  ';
    }
    s += path;
    log(s);
  }
  if (total) {
    const sz = human ? `${fmtSize(nTotalSize)}B` : `${comma(nTotalSize)} bytes`;
    log(`\n${sz} in ${comma(nTotalCount)} file${nTotalCount > 1 ? 's' : ''}`);
  }
}

const STORAGE = {
  STANDARD: 'S',
  STANDARD_IA: 'I',
  GLACIER: 'G',
  DEEP_ARCHIVE: 'D'
};

const localList = sql(`
  SELECT *
  FROM local_file_view
  WHERE path LIKE $path || '%'
  ORDER BY path
`);

const s3List = sql(`
  SELECT *
  FROM s3_file_view
  WHERE bucket = $bucket
  AND   path LIKE $path || '%'
  ORDER BY bucket, path
`);

function speedo ({
  total,
  interval = 250,
  windowSize = 40
} = {}) {
  let readings;
  let start;
  return Object.assign(transform, { current: 0, total, update, done: false })

  async function * transform (source) {
    start = Date.now();
    readings = [[start, 0]];
    const int = setInterval(update, interval);
    try {
      for await (const chunk of source) {
        transform.current += chunk.length;
        yield chunk;
      }
      transform.total = transform.current;
      update(true);
    } finally {
      clearInterval(int);
    }
  }

  function update (done = false) {
    if (transform.done) return
    const { current, total } = transform;
    const now = Date.now();
    const taken = now - start;
    readings = [...readings, [now, current]].slice(-windowSize);
    const first = readings[0];
    const wl = current - first[1];
    const wt = now - first[0];
    const rate = 1e3 * (done ? total / taken : wl / wt);
    const percent = Math.round((100 * current) / total);
    const eta = done || !total ? 0 : (1e3 * (total - current)) / rate;
    Object.assign(transform, { done, taken, rate, percent, eta });
  }
}

// import assert from 'assert/strict'
function throttle (options) {
  if (typeof options !== 'object') options = { rate: options };
  const { chunkTime = 100, windowSize = 30 } = options;
  const rate = getRate(options.rate);
  return async function * throttle (source) {
    let window = [[0, Date.now()]];
    let bytes = 0;
    let chunkBytes = 0;
    const chunkSize = Math.max(1, Math.ceil((rate * chunkTime) / 1e3));
    for await (let data of source) {
      while (data.length) {
        const chunk = data.slice(0, chunkSize - chunkBytes);
        data = data.slice(chunk.length);
        chunkBytes += chunk.length;
        if (chunkBytes < chunkSize) {
          // assert.equal(data.length, 0)
          yield chunk;
          continue
        }
        bytes += chunkSize;
        // assert.equal(chunkBytes, chunkSize)
        chunkBytes = 0;
        const now = Date.now();
        const first = window[0];
        const eta = first[1] + (1e3 * (bytes - first[0])) / rate;
        window = [...window, [bytes, Math.max(now, eta)]].slice(-windowSize);
        if (now < eta) {
          await delay(eta - now);
        }
        yield chunk;
      }
    }
  }
}

function getRate (val) {
  const n = (val + '').toLowerCase();
  if (!/^\d+[mk]?$/.test(n)) throw new Error(`Invalid rate: ${val}`)
  const m = n.endsWith('m') ? 1024 * 1024 : n.endsWith('k') ? 1024 : 1;
  return parseInt(n) * m
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

function progressStream ({
  onProgress,
  interval = 1000,
  ...rest
} = {}) {
  return async function * transform (source) {
    const int = setInterval(report, interval);
    let bytes = 0;
    let done = false;
    try {
      for await (const chunk of source) {
        bytes += chunk.length;
        yield chunk;
      }
      done = true;
      report();
    } finally {
      clearInterval(int);
    }

    function report () {
      onProgress && onProgress({ bytes, done, ...rest });
    }
  }
}

async function upload (source, dest, opts) {
  const { path, size, contentType, md5Hash } = source;
  const { dryRun, limit, progress, interval = 1000 } = opts;
  if (dryRun) {
    log.colour('cyan')('%s uploaded (dryrun)', dest.url);
    return
  }

  if (progress) log(log.cyan(dest.url));

  const speedo$1 = speedo({ total: size });
  const body = new PassThrough();

  const pPipeline = pipeline(
    ...[
      createReadStream(path),
      limit && throttle(limit),
      progress && speedo$1,
      progress && progressStream({ onProgress, interval, speedo: speedo$1 }),
      body
    ].filter(Boolean)
  );

  const request = {
    Body: body,
    Bucket: dest.bucket,
    Key: dest.path,
    ContentLength: size,
    ContentType: contentType,
    ContentMD5: Buffer.from(md5Hash, 'hex').toString('base64'),
    Metadata: makeMetadata(source)
  };

  // perform the upload
  const s3 = getS3();
  const pUpload = s3.putObject(request).promise();

  // wait for everything to finish
  await Promise.all([pPipeline, pUpload]);
  const { ETag } = await pUpload;

  // check the etag is the md5 of the source data
  /* c8 ignore next 3 */
  if (ETag !== `"${md5Hash}"`) {
    throw new Error(`Upload of ${path} to ${dest} failed`)
  }

  dest.md5Hash = undefined; // force re-stat
  await dest.stat();
  insertFile(dest);
}

function makeMetadata ({ mtime, size, md5Hash, contentType }) {
  const ms = new Date(mtime + 'Z').getTime();
  let md = {};
  md = { ...md, uname: 'alan', gname: 'alan', uid: 1000, gid: 1000 };
  md = { ...md, atime: ms, ctime: ms, mtime: ms };
  md = { ...md, size, mode: 0o644, md5: md5Hash, contentType };
  return {
    's3cmd-attrs': Object.keys(md)
      .sort()
      .map(k => `${k}:${md[k]}`)
      .join('/')
  }
}

function hashStream ({ algo = 'md5', enc = 'hex' } = {}) {
  return async function * transform (source) {
    const hasher = createHash(algo);
    for await (const chunk of source) {
      hasher.update(chunk);
      yield chunk;
    }
    transform.hash = hasher.digest(enc);
  }
}

async function download (source, dest, opts = {}) {
  const { bucket, path, size, mtime, md5Hash, storage } = source;
  const { dryRun, progress, interval = 1000, limit } = opts;

  if (!storage.toLowerCase().startsWith('standard')) {
    throw new Error(`${source.url} needs to be restored for copy`)
  }

  if (dryRun) {
    log.colour('cyan')('%s downloaded (dryrun)', dest.path);
    return
  }

  if (progress) log(log.cyan(dest.path));

  await mkdir(dirname(dest.path), { recursive: true });

  const s3 = getS3();
  const hasher = hashStream();
  const speedo$1 = speedo({ total: size });
  const streams = [
    s3.getObject({ Bucket: bucket, Key: path }).createReadStream(),
    hasher,
    limit && throttle(limit),
    progress && speedo$1,
    progress && progressStream({ onProgress, interval, speedo: speedo$1 }),
    createWriteStream(dest.path)
  ].filter(Boolean);

  await pipeline(...streams);
  /* c8 ignore next 3 */
  if (hasher.hash !== md5Hash) {
    throw new Error(`Error downloading ${source.url} to ${dest.path}`)
  }

  const tm = new Date(mtime + 'Z');
  await utimes(dest.path, tm, tm);

  insertFile$1({ ...dest, md5Hash, size, mtime });
}

async function cp (src, dst, opts = {}) {
  src = File.fromUrl(src, { resolve: true });
  dst = File.fromUrl(dst, { resolve: true });
  const dir = getDirection(src, dst);
  const fns = {
    local_s3: upload,
    s3_local: download
  };
  const fn = fns[dir];

  await src.stat();
  await fn(src, dst, opts);

  if (!opts.dryRun && !opts.progress && !opts.quiet) log(dst.url);
}

async function copy$1 (from, to, opts = {}) {
  const { dryRun } = opts;
  if (dryRun) {
    log(log.blue(from.path));
    log(log.cyan(` -> ${to.path} copied (dryrun)`));
    return
  }

  await mkdir(dirname(to.path), { recursive: true });
  await copyFile(from.path, to.path);
  if (typeof from.mtime === 'string') from.mtime = new Date(from.mtime + 'Z');
  await utimes(to.path, from.mtime, from.mtime);

  log(log.blue(from.path));
  log(log.cyan(` -> ${to.path} copied`));

  insertFile$1({ ...from, path: to.path });
}

async function remove$1 (file, opts) {
  const { dryRun } = opts;
  if (dryRun) {
    log(log.cyan(`${file.path} deleted (dryrun)`));
    return
  }
  await unlink(file.path);
  let dir = dirname(file.path);
  while (true) {
    try {
      await rmdir(dir);
    } catch (err) {
      if (err.code === 'ENOTEMPTY') break
      throw err
    }
    dir = dirname(dir);
  }
  removeFile$1(file);
  log(log.cyan(`${file.path} deleted`));
}

async function copy (from, to, opts = {}) {
  const { dryRun } = opts;

  if (dryRun) {
    log(log.blue(from.url));
    log(log.cyan(` -> ${to.url} copied (dryrun)`));
    return
  }

  const s3 = getS3();
  await s3
    .copyObject({
      Bucket: to.bucket,
      Key: to.path,
      CopySource: `${from.bucket}/${from.path}`,
      MetadataDirective: 'COPY'
    })
    .promise();

  await to.stat();
  insertFile(to);

  log(log.blue(from.url));
  log(log.cyan(` -> ${to.url} copied`));
}

async function remove (file, opts) {
  const { dryRun } = opts;

  if (dryRun) {
    log(log.cyan(`${file.url} removed (dryrun)`));
    return
  }

  const s3 = getS3();
  await s3.deleteObject({ Bucket: file.bucket, Key: file.path }).promise();
  removeFile(file);
  log(log.cyan(`${file.url} removed`));
}

async function sync (srcRoot, dstRoot, opts = {}) {
  srcRoot = File.fromUrl(srcRoot, { resolve: true, directory: true });
  dstRoot = File.fromUrl(dstRoot, { resolve: true, directory: true });
  const fn = getFunctions(srcRoot, dstRoot);

  await srcRoot.scan();
  await dstRoot.scan();

  const seen = new Set();

  // new files
  for (const row of fn.newFiles(fn.paths)) {
    const src = File.like(srcRoot, row);
    const dst = src.rebase(srcRoot, dstRoot);
    await fn.copy(src, dst, { ...opts, progress: true });
    seen.add(dst.path);
  }

  // simple renames
  for (const { contentId } of fn.differences(fn.paths)) {
    const d = { contentId, ...fn.paths };
    const src = File.like(srcRoot, fn.srcContent.get(d));
    const dst = src.rebase(srcRoot, dstRoot);
    const old = File.like(dstRoot, fn.dstContent.get(d));
    if (!old.archived) {
      await fn.destCopy(old, dst, opts);
    } else {
      await fn.copy(src, dst, { ...opts, progress: true });
    }
    seen.add(dst.path);
  }

  // complex renames
  for (const { contentId } of fn.duplicates()) {
    const d = { contentId, ...fn.paths };
    const srcs = fn.srcContent.all(d).map(row => File.like(srcRoot, row));
    const dsts = fn.dstContent.all(d).map(row => File.like(dstRoot, row));
    for (const src of srcs) {
      const dst = src.rebase(srcRoot, dstRoot);
      if (!dsts.find(d => d.url === dst.url)) {
        const cpy = dsts.find(d => !d.archived);
        if (cpy) {
          await fn.destCopy(cpy, dst, opts);
        } else {
          await fn.copy(src, dst, { ...opts, progress: true });
        }
        seen.add(dst.path);
      }
    }

    if (opts.delete) {
      for (const dst of dsts) {
        const src = dst.rebase(dstRoot, srcRoot);
        if (!srcs.find(s => s.url === src.url)) {
          await fn.remove(dst, opts);
          seen.add(dst.path);
        }
      }
    }
  }

  // deletes
  if (opts.delete) {
    for (const row of fn.oldFiles(fn.paths)) {
      const dst = File.like(dstRoot, row);
      if (!seen.has(dst.path)) {
        await fn.remove(dst, opts);
      }
    }
  }
}

function getFunctions (src, dst) {
  const dir = getDirection(src, dst);
  const paths = {
    localPath: (src.isLocal && src.path) || (dst.isLocal && dst.path),
    s3Bucket: (src.isS3 && src.bucket) || (dst.isS3 && dst.bucket),
    s3Path: (src.isS3 && src.path) || (dst.isS3 && dst.path)
  };
  return {
    local_s3: {
      paths,
      newFiles: localNotS3.all,
      oldFiles: s3NotLocal.all,
      differences: localS3Diff.all,
      duplicates: duplicates.all,
      srcContent: localContent,
      dstContent: s3Content,
      copy: upload,
      destCopy: copy,
      remove: remove
    },
    s3_local: {
      paths,
      newFiles: s3NotLocal.all,
      oldFiles: localNotS3.all,
      differences: localS3Diff.all,
      duplicates: duplicates.all,
      srcContent: s3Content,
      dstContent: localContent,
      copy: download,
      destCopy: copy$1,
      remove: remove$1
    }
  }[dir]
}

const localNotS3 = sql(`
  SELECT * FROM local_file_view
  WHERE path LIKE $localPath || '%'
  AND contentId NOT IN (
    SELECT contentId
    FROM s3_file
    WHERE bucket = $s3Bucket
    AND   path LIKE $s3Path || '%'
  )
`);

const s3NotLocal = sql(`
  SELECT * FROM s3_file_view
  WHERE bucket = $s3Bucket
  AND   path LIKE $s3Path || '%'
  AND   contentId NOT IN (
    SELECT contentId
    FROM local_file
    WHERE path LIKE $localPath || '%'
  )
`);

const localS3Diff = sql(`
  SELECT * FROM local_and_s3_view
  WHERE localPath LIKE $localPath || '%'
  AND   s3Bucket = $s3Bucket
  AND   s3Path LIKE $s3Path || '%'
  AND   substr(localPath, 1 + length($localPath)) !=
          substr(s3Path, 1 + length($s3Path))
`);

const duplicates = sql(`
  SELECT contentId FROM duplicates_view
`);

const localContent = sql(`
  SELECT * FROM local_file_view
  WHERE contentId = $contentId
  AND   path LIKE $localPath || '%'
`);

const s3Content = sql(`
  SELECT * FROM s3_file_view
  WHERE contentId = $contentId
  AND   bucket = $s3Bucket
  AND   path LIKE $s3Path || '%'
`);

async function rm (file, opts = {}) {
  file = File.fromUrl(file, { resolve: true });
  const fns = {
    local: remove$1,
    s3: remove
  };
  const fn = fns[file.type];

  await file.stat();
  await fn(file, opts);
}

const prog = sade('s3cli');
const version = '2.1.6';

prog.version(version);

prog
  .command('ls <url>', 'list files')
  .option('-r --rescan', 'rescan before listing')
  .option('-l --long', 'show long listing')
  .option('-H --human', 'show amount in human sizes')
  .option('-t --total', 'show grand total')
  .action(ls);

prog
  .command('cp <from> <to>', 'copy a file to/from S3')
  .option('-p, --progress', 'show progress')
  .option('-l, --limit', 'limit rate')
  .option('-q, --quiet', 'no output')
  .action(cp);

prog
  .command('sync <from> <to>', 'sync a directory to/from S3')
  .option('-l, --limit', 'limit rate')
  .option('-n, --dry-run', 'show what would be done')
  .option('-d, --delete', 'delete extra files on the destination')
  .action(sync);

prog
  .command('rm <url>')
  .describe('delete a file')
  .action(rm);

const parsed = prog.parse(process.argv, {
  lazy: true,
  alias: { n: ['dryRun', 'dry-run'] }
});

if (parsed) {
  const { args, handler } = parsed;
  handler(...args).catch(err => {
    console.error(err);
    process.exit(1);
  });
}
