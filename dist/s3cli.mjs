#!/usr/bin/env node
import sade from 'sade';
import { homedir } from 'os';
import { resolve, join, dirname } from 'path';
import SQLite from 'better-sqlite3';
import { format } from 'util';
import { createReadStream, realpathSync, createWriteStream } from 'fs';
import mime from 'mime';
import { readdir, lstat, mkdir, utimes, rename as rename$2, rmdir, unlink } from 'fs/promises';
import { createHash } from 'crypto';
import AWS from 'aws-sdk';
import { PassThrough } from 'stream';
import { pipeline } from 'stream/promises';

let db;

const SQL = {
  from: sql => statement({ sql: tidy(sql) }),
  attach: _db => (db = _db),
  transaction: fn => transaction(fn)
};

function statement (data) {
  function exec (...args) {
    args = args.map(cleanArgs);
    if (!data.sqlList) data.sqlList = data.sql.split(';');
    if (!data.prepared) data.prepared = [];
    const { prepared, sqlList, pluck, raw, get, all } = data;
    const n = sqlList.length;
    for (let i = 0; i < n - 1; i++) {
      let stmt = prepared[i];
      if (!stmt) stmt = prepared[i] = db.prepare(sqlList[i]);
      stmt.run(...args);
    }
    let last = prepared[n - 1];
    if (!last) last = prepared[n - 1] = db.prepare(sqlList[n - 1]);
    if (pluck) last = last.pluck();
    if (raw) last = last.raw();
    if (get) return last.get(...args)
    if (all) return last.all(...args)
    return last.run(...args)
  }
  return Object.defineProperties(exec, {
    pluck: { value: () => statement({ ...data, pluck: true }) },
    raw: { value: () => statement({ ...data, raw: true }) },
    get: { get: () => statement({ ...data, get: true }) },
    all: { get: () => statement({ ...data, all: true }) }
  })
}

function cleanArgs (x) {
  if (!x || typeof x !== 'object') return x
  x = { ...x };
  for (const k in x) {
    if (x[k] instanceof Date) x[k] = x[k].toISOString();
  }
  return x
}

function transaction (_fn) {
  let fn;
  return (...args) => {
    if (!fn) fn = db.transaction(_fn);
    return fn(...args)
  }
}

function tidyStatement (statement) {
  return statement
    .split('\n')
    .map(line => line.trim())
    .filter(line => !line.startsWith('--'))
    .map(line => line.replaceAll(/  +/g, ' '))
    .join(' ')
    .trim()
}

function tidy (statements) {
  return statements
    .split(';')
    .map(tidyStatement)
    .filter(Boolean)
    .join(';')
}

const cleanup=SQL.from("DELETE FROM s3_file WHERE updated IS NULL;DELETE FROM local_file WHERE updated IS NULL;DELETE FROM content WHERE contentId NOT IN ( SELECT contentId FROM s3_file UNION SELECT contentId FROM local_file )");
const clearFilesBeforeScan=SQL.from("UPDATE s3_file SET updated = NULL WHERE $url LIKE 's3://%' AND 's3://' || bucket || '/' || path LIKE $url || '%';UPDATE local_file SET updated = NULL WHERE $url LIKE 'file://%' AND 'file://' || path LIKE $url || '%'");
const ddl=SQL.from("PRAGMA journal_mode = WAL;PRAGMA foreign_keys = ON;CREATE VIEW IF NOT EXISTS dbVersion AS SELECT 4 AS version;CREATE TABLE IF NOT EXISTS content( contentId INTEGER PRIMARY KEY NOT NULL, md5Hash TEXT NOT NULL, size INTEGER NOT NULL, contentType TEXT, updated TEXT DEFAULT (datetime('now')), UNIQUE (md5Hash, size) );CREATE TABLE IF NOT EXISTS s3_file( bucket TEXT NOT NULL, path TEXT NOT NULL, contentId INTEGER NOT NULL REFERENCES content(contentId), mtime TEXT NOT NULL, storage TEXT NOT NULL, updated TEXT DEFAULT (datetime('now')), PRIMARY KEY (bucket, path) );CREATE TABLE IF NOT EXISTS local_file( path TEXT NOT NULL PRIMARY KEY, contentId INTEGER NOT NULL REFERENCES content(contentId), mtime TEXT NOT NULL, updated TEXT DEFAULT (datetime('now')) );CREATE VIEW IF NOT EXISTS s3_file_view AS SELECT f.bucket AS bucket, f.path AS path, c.size AS size, f.mtime AS mtime, f.storage AS storage, c.contentType AS contentType, c.md5Hash AS md5Hash FROM s3_file f JOIN content c USING (contentId) ORDER BY f.bucket, f.path;CREATE VIEW IF NOT EXISTS local_file_view AS SELECT f.path AS path, c.size AS size, f.mtime AS mtime, c.contentType AS contentType, c.md5Hash AS md5Hash FROM local_file f JOIN content c USING (contentId) ORDER BY f.path;CREATE VIEW IF NOT EXISTS content_use_view AS SELECT c.contentId AS contentId, count(l.contentId) AS local_use, count(r.contentId) AS remote_use  FROM content c  LEFT JOIN local_file l USING (contentId)  LEFT JOIN s3_file r USING (contentId)  GROUP BY c.contentId");
const findDifferentPaths=SQL.from("WITH loc_path AS ( SELECT path, substr(path, length($localRoot) - 6) AS rel_path FROM local_file ),  rem_path AS ( SELECT bucket, path, substr(path, length($s3Root) - length(bucket) - 5) AS rel_path FROM s3_file )  SELECT lp.rel_path AS localPath, l.mtime AS localMtime, rp.rel_path AS remotePath, r.mtime AS remoteMtime, r.storage AS storage, c.size AS size, c.contentType AS contentType, c.md5Hash AS md5Hash  FROM local_file l JOIN s3_file r USING (contentId) JOIN content c USING (contentId) JOIN content_use_view u USING (contentId)  JOIN loc_path lp ON lp.path = l.path  JOIN rem_path rp ON rp.bucket = r.bucket AND rp.path = r.path  WHERE 'file://' || l.path LIKE $localRoot || '%' AND 's3://' || r.bucket || '/' || r.path LIKE $s3Root || '%' AND lp.rel_path != rp.rel_path AND u.remote_use = 1 AND u.local_use = 1  ORDER BY lp.rel_path");
const findDuplicates=SQL.from("SELECT contentId FROM content_use_view WHERE remote_use > 1 OR local_use > 1");
const findLocalContent=SQL.from("SELECT 'local' AS type, f.path AS path, f.mtime AS mtime, c.size AS size, c.md5Hash AS md5Hash, c.contentType AS contentType  FROM local_file f JOIN content c USING (contentId)  WHERE f.contentId = $contentId AND 'file://' || f.path LIKE $localRoot || '%'  ORDER BY f.path");
const findLocalNotRemote=SQL.from("SELECT f.path AS path, f.mtime AS mtime, c.size AS size, c.contentType AS contentType, c.md5Hash AS md5Hash  FROM local_file f JOIN content c USING (contentId) JOIN content_use_view u USING (contentId)  WHERE u.remote_use = 0 AND u.local_use = 1 AND 'file://' || f.path LIKE $localRoot || '%'  ORDER BY f.path");
const findRemoteContent=SQL.from("SELECT 's3' AS type, f.bucket AS bucket, f.path AS path, f.mtime AS mtime, f.storage AS storage, c.size AS size, c.md5Hash AS md5Hash, c.contentType AS contentType  FROM s3_file f JOIN content c USING (contentId)  WHERE f.contentId = $contentId AND 's3://' || f.bucket || '/' || f.path LIKE $s3Root || '%'  ORDER BY f.bucket, f.path");
const findRemoteNotLocal=SQL.from("SELECT f.bucket AS bucket, f.path AS path, f.mtime AS mtime, c.size AS size, c.contentType AS contentType, c.md5Hash AS md5Hash, f.storage AS storage  FROM s3_file f JOIN content c USING (contentId) JOIN content_use_view u USING (contentId)  WHERE u.local_use = 0 AND u.remote_use = 1 AND 's3://' || f.bucket || '/' || f.path LIKE $s3Root || '%'  ORDER BY f.bucket, f.path");
const insertLocalFile=SQL.from("INSERT INTO content (md5Hash, size, contentType) VALUES ($md5Hash, $size, $contentType)  ON CONFLICT DO UPDATE SET contentType = excluded.contentType, updated = excluded.updated;INSERT INTO local_file (path, contentId, mtime) SELECT $path, contentId, datetime($mtime) FROM content WHERE md5Hash = $md5Hash AND size = $size  ON CONFLICT DO UPDATE SET contentId = excluded.contentId, mtime = excluded.mtime, updated = excluded.updated");
const insertS3File=SQL.from("INSERT INTO content (md5Hash, size, contentType) VALUES ($md5Hash, $size, $contentType)  ON CONFLICT DO UPDATE SET contentType = excluded.contentType, updated = excluded.updated;INSERT INTO s3_file (bucket, path, contentId, mtime, storage) SELECT $bucket, $path, contentId, datetime($mtime), $storage FROM content WHERE md5Hash = $md5Hash AND size = $size  ON CONFLICT DO UPDATE SET contentId = excluded.contentId, mtime = excluded.mtime, storage = excluded.storage, updated = excluded.updated");
const listLocalFiles=SQL.from("SELECT path AS path, size AS size, mtime AS mtime, contentType AS contentType, NULL AS storage, md5Hash AS md5Hash  FROM local_file_view  WHERE path LIKE $path || '%'  ORDER BY path");
const listS3files=SQL.from("SELECT bucket AS bucket, path AS path, size AS size, mtime AS mtime, contentType AS contentType, storage AS storage, md5Hash AS md5Hash  FROM s3_file_view  WHERE bucket = $bucket AND path LIKE $path || '%'  ORDER BY bucket, path");
const moveLocalFile=SQL.from("UPDATE local_file SET path = $newPath WHERE path = $oldPath");
const moveS3file=SQL.from("UPDATE s3_file  SET path = $newPath  WHERE bucket = $bucket AND path = $oldPath");
const removeLocalFile=SQL.from("DELETE FROM local_file WHERE path = $path");
const removeS3file=SQL.from("DELETE FROM s3_file WHERE bucket = $bucket AND path = $path");
const selectLocalHash=SQL.from("SELECT c.md5Hash  FROM content c JOIN local_file f USING (contentId)  WHERE f.path = $path AND c.size = $size AND f.mtime = datetime($mtime)");

const DBVERSION = 4;

let opened;
function open () {
  if (opened) return
  opened = true;
  const dbFile =
    process.env.DB || resolve(homedir(), '.databases', 'files4.sqlite');
  const db = new SQLite(dbFile);
  SQL.attach(db);
  ddl();
  const version = db
    .prepare('select version from dbversion')
    .pluck()
    .get();
  if (version !== DBVERSION) {
    throw new Error('Wrong version of database: ' + dbFile)
  }
}

const insertS3Files = SQL.transaction(files =>
  files.forEach(file => insertS3File(file))
);

const insertLocalFiles = SQL.transaction(files =>
  files.forEach(file => insertLocalFile(file))
);

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

async function * scan$1 (root) {
  yield * scanDir(root.path);
}

async function * scanDir (dir) {
  const File = (await Promise.resolve().then(function () { return file; })).default;
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
    yield * scanDir(dir);
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
  file.md5Hash = selectLocalHash.pluck().get(file);
  if (!file.md5Hash) {
    log.status('%s ... hashing', file.path);
    file.md5Hash = await hashFile(file.path);
    insertLocalFile(file);
  }
}

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

function isUploading (src, dst) {
  if (src.isLocal && dst.isS3) return true
  if (src.isS3 && dst.isLocal) return false
  throw new Error('Must either upload or download')
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

async function * scan (root) {
  const File = (await Promise.resolve().then(function () { return file; })).default;
  const s3 = getS3();
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
    yield files;

    if (!result.IsTruncated) break
    request.ContinuationToken = result.NextContinuationToken;
  }
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
    let n = 0;
    log.status('Scanning %s ... ', this.url);
    clearFilesBeforeScan({ url: this.url });
    const scanner = this.isLocal ? scan$1 : scan;
    const insert = this.isLocal ? insertLocalFiles : insertS3Files;
    for await (const files of scanner(this)) {
      n += files.length;
      log.status('Scanning %s ... %d', this.url, n);
      insert(files);
    }
    log('%s files found on %s', n.toLocaleString(), this.url);
    cleanup();
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

var file = /*#__PURE__*/Object.freeze({
  __proto__: null,
  'default': File
});

async function ls (url, opts) {
  const { long, rescan, human, total } = opts;
  url = File.fromUrl(url, { resolve: true });

  if (rescan) await url.scan();

  let nTotalCount = 0;
  let nTotalSize = 0;

  const sql = url.isLocal ? listLocalFiles : listS3files;
  for (const row of sql.all(url)) {
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

  dest.md5Hash = undefined;
  await dest.stat();
  insertS3File(dest);
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

  insertLocalFile({ ...dest, md5Hash, size, mtime });
}

async function cp (src, dst, opts = {}) {
  src = File.fromUrl(src, { resolve: true });
  dst = File.fromUrl(dst, { resolve: true });
  const uploading = isUploading(src, dst);

  await src.stat();

  if (uploading) {
    await upload(src, dst, opts);
  } else {
    await download(src, dst, opts);
  }

  if (!opts.dryRun && !opts.progress && !opts.quiet) log(dst.url);
}

async function rename$1 (from, to, opts = {}) {
  const { dryRun } = opts;
  if (dryRun) {
    log(log.blue(from.path));
    log(log.cyan(` -> ${to.path} renamed (dryrun)`));
    return
  }

  await mkdir(dirname(to.path), { recursive: true });
  await rename$2(from.path, to.path);
  let dir = dirname(from.path);
  while (true) {
    try {
      await rmdir(dir);
    } catch (err) {
      if (err.code === 'ENOTEMPTY') break
      throw err
    }
    dir = dirname(dir);
  }
  log(log.blue(from.path));
  log(log.cyan(` -> ${to.path} renamed`));

  moveLocalFile({ oldPath: from.path, newPath: to.path });
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
  removeLocalFile(file);
  log(log.cyan(`${file.path} deleted`));
}

async function rename (from, to, opts = {}) {
  const { dryRun } = opts;

  if (dryRun) {
    log(log.blue(from.url));
    log(log.cyan(` -> ${to.url} renamed (dryrun)`));
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

  await s3
    .deleteObject({
      Bucket: from.bucket,
      Key: from.path
    })
    .promise();
  log(log.blue(from.url));
  log(log.cyan(` -> ${to.url} renamed`));
  moveS3file({
    bucket: from.bucket,
    oldPath: from.path,
    newPath: to.path
  });
}

async function remove (file, opts) {
  const { dryRun } = opts;

  if (dryRun) {
    log(log.cyan(`${file.url} removed (dryrun)`));
    return
  }

  const s3 = getS3();
  await s3.deleteObject({ Bucket: file.bucket, Key: file.path }).promise();
  removeS3file(file);
  log(log.cyan(`${file.url} removed`));
}

async function sync (srcRoot, dstRoot, opts = {}) {
  srcRoot = File.fromUrl(srcRoot, { resolve: true, directory: true });
  dstRoot = File.fromUrl(dstRoot, { resolve: true, directory: true });
  const uploading = isUploading(srcRoot, dstRoot);

  await srcRoot.scan();
  await dstRoot.scan();

  const updatedFiles = new Set();

  const roots = {
    localRoot: uploading ? srcRoot.url : dstRoot.url,
    s3Root: uploading ? dstRoot.url : srcRoot.url
  };

  // add in new from source
  const sql = uploading ? findLocalNotRemote : findRemoteNotLocal;

  for (const row of sql.all(roots)) {
    const type = uploading ? 'local' : 's3';
    const src = new File({ type, ...row });
    const dest = src.rebase(srcRoot, dstRoot);
    if (uploading) {
      await upload(src, dest, { ...opts, progress: true });
    } else {
      await download(src, dest, { ...opts, progress: true });
    }
    updatedFiles.add(dest.url);
  }

  // rename files on destination
  for (const row of findDifferentPaths.all(roots)) {
    const { local, remote } = getDifferentFiles(row, srcRoot, dstRoot);
    if (uploading) {
      const dest = local.rebase(srcRoot, dstRoot);
      if (remote.storage.toLowerCase().startsWith('standard')) {
        await rename(remote, dest, opts);
      } else {
        await upload(local, dest, { ...opts, progress: true });
        await remove(remote);
      }
      updatedFiles.add(dest.url);
    } else {
      const dest = remote.rebase(srcRoot, dstRoot);
      await rename$1(local, dest, opts);
      updatedFiles.add(dest.url);
    }
  }

  // handle complex (multi-copy) matches
  for (const contentId of findDuplicates.pluck().all()) {
    const local = findLocalContent
      .all({ contentId, ...roots })
      .map(row => new File(row));
    const remote = findRemoteContent
      .all({ contentId, ...roots })
      .map(row => new File(row));
    const [src, dst] = uploading ? [local, remote] : [remote, local];
    const seen = new Set();
    for (const s of src) {
      const d = s.rebase(srcRoot, dstRoot);
      if (!dst.find(f => f.url === d.url)) {
        if (uploading) {
          await upload(s, d, { ...opts, progress: true });
        } else {
          await download(s, d, { ...opts, progress: true });
        }
      }
      seen.add(d.url);
    }

    if (opts.delete) {
      for (const d of dst) {
        if (!seen.has(d.url)) {
          if (uploading) {
            await remove(d, opts);
          } else {
            await remove$1(d, opts);
          }
        }
      }
    }
  }

  // delete extra from destination
  if (opts.delete) {
    const sql = uploading ? findRemoteNotLocal : findLocalNotRemote;
    const type = uploading ? 's3' : 'local';
    for (const row of sql.all(roots)) {
      const file = new File({ type, ...row });
      if (updatedFiles.has(file.url)) continue
      if (uploading) {
        await remove(file, opts);
      } else {
        await remove$1(file, opts);
      }
    }
  }
  cleanup();
}

function getDifferentFiles (row, srcRoot, dstRoot) {
  const localRoot = srcRoot.isLocal ? srcRoot : dstRoot;
  const remoteRoot = srcRoot.isS3 ? srcRoot : dstRoot;
  const local = new File({
    type: 'local',
    path: localRoot.path + row.localPath,
    mtime: row.localMtime,
    size: row.size,
    contentType: row.contentType,
    md5Hash: row.md5Hash
  });
  const remote = new File({
    type: 's3',
    bucket: remoteRoot.bucket,
    path: remoteRoot.path + row.remotePath,
    storage: row.storage,
    mtime: row.remoteMtime,
    size: row.size,
    contentType: row.contentType,
    md5Hash: row.md5Hash
  });
  return { local, remote }
}

async function rm (file, opts = {}) {
  file = File.fromUrl(file, { resolve: true });
  await file.stat();
  if (file.isS3) {
    await remove(file, opts);
  } else {
    await remove$1(file, opts);
  }
}

const prog = sade('s3cli');
const version = '2.1.2';

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
  open();
  const { args, handler } = parsed;
  handler(...args).catch(err => {
    console.error(err);
    process.exit(1);
  });
}
