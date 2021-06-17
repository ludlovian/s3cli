#!/usr/bin/env node
import sade from 'sade';
import { pipeline } from 'stream/promises';
import EventEmitter, { EventEmitter as EventEmitter$1 } from 'events';
import { PassThrough } from 'stream';
import AWS from 'aws-sdk';
import { homedir } from 'os';
import { resolve, extname } from 'path';
import SQLite from 'better-sqlite3';
import { realpath, readdir, stat as stat$1, chmod, utimes, unlink } from 'fs/promises';
import { createReadStream as createReadStream$2, createWriteStream as createWriteStream$2 } from 'fs';
import mime from 'mime';
import { createHash } from 'crypto';
import { format as format$1 } from 'util';

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

const ddl = t`
  CREATE TABLE IF NOT EXISTS hash (
    url TEXT NOT NULL PRIMARY KEY,
    mtime TEXT NOT NULL,
    size INTEGER NOT NULL,
    hash TEXT
  );
  DROP TABLE IF EXISTS sync;
  CREATE TEMP TABLE IF NOT EXISTS sync (
    type TEXT NOT NULL,
    path TEXT NOT NULL,
    url  TEXT NOT NULL,
    mtime TEXT NOT NULL,
    size INTEGER NOT NULL,
    PRIMARY KEY (type, path)
  );
`;
const sql = {};

sql.insertHash = t`
  INSERT INTO hash
    (url, mtime, size, hash)
  VALUES
    ($url, $mtime, $size, $hash)
  ON CONFLICT DO UPDATE
  SET mtime = excluded.mtime,
      size  = excluded.size,
      hash  = excluded.hash
`;

sql.insertSync = t`
  INSERT INTO sync
    (type, path, url, mtime, size)
  VALUES
    ($type, $path, $url, $mtime, $size)
`;

sql.selectHash = t`
  SELECT hash
    FROM hash
    WHERE url = $url
      AND mtime = $mtime
      AND size = $size
`;

sql.selectMissingFiles = t`
  SELECT a.url,
         a.path
    FROM sync a
    LEFT JOIN sync b
      ON  a.path = b.path
      AND b.type = 'dst'
    WHERE a.type = 'src'
      AND b.path IS NULL
    ORDER BY a.url
`;

sql.selectMissingHashes = t`
  SELECT a.url
    FROM sync a
    LEFT JOIN hash b
      ON a.url    = b.url
      AND a.mtime = b.mtime
      AND a.size  = b.size
    WHERE b.hash IS NULL
    ORDER BY a.url
`;

sql.selectChanged = t`
  SELECT a.url AS "from",
         b.url AS "to"
    FROM sync a
    JOIN sync b ON b.path = a.path
    JOIN hash c ON c.url = a.url
    JOIN hash d ON d.url = b.url
    WHERE a.type = 'src'
      AND b.type = 'dst'
      AND c.hash != d.hash
    ORDER BY a.url
`;

sql.selectSurplusFiles = t`
  SELECT b.url AS url
    FROM sync b
    LEFT JOIN sync a
      ON  a.path = b.path
      AND a.type = 'src'
    WHERE b.type = 'dst'
      AND a.path IS NULL
    ORDER BY b.url
`;

sql.countFiles = t`
  SELECT count(*)
    FROM sync
    WHERE type = 'src'
`;

function t (strings) {
  return strings
    .map(s => s.split('\n'))
    .flat()
    .map(x => x.trim())
    .join(' ')
}

const DB_DIR = process.env.DB_DIR || resolve(homedir(), '.databases');
const DB_FILE = process.env.DB_FILE || 'files2.sqlite';

const db = new SQLite(resolve(DB_DIR, DB_FILE));
db.pragma('journal_mode=WAL');
db.exec(ddl);

for (const k in sql) sql[k] = db.prepare(sql[k]);

db.transaction(hashes => {
  for (const { url, mtime: _mtime, size, hash } of hashes) {
    const mtime = _mtime.toISOString();
    sql.insertHash.run({ url, mtime, size, hash });
  }
});

const insertSyncFiles = db.transaction((type, files) => {
  for (const { path, url, mtime: _mtime, size } of files) {
    const mtime = _mtime.toISOString();
    sql.insertSync.run({ type, path, url, mtime, size });
  }
});

function selectMissingFiles () {
  return sql.selectMissingFiles.all()
}

function selectMissingHashes () {
  return sql.selectMissingHashes.pluck().all()
}

function selectChanged () {
  return sql.selectChanged.all()
}

function selectSurplusFiles () {
  return sql.selectSurplusFiles.pluck().all()
}

function countFiles () {
  return sql.countFiles.pluck().get()
}

function selectHash ({ url, mtime: _mtime, size }) {
  const mtime = _mtime.toISOString();
  return sql.selectHash.pluck().get({ url, mtime, size })
}

function insertHash ({ url, mtime: _mtime, size, hash }) {
  const mtime = _mtime.toISOString();
  sql.insertHash.run({ url, mtime, size, hash });
}

const getS3 = once(async () => {
  const REGION = 'eu-west-1';
  return new AWS.S3({ region: REGION })
});

function parseAddress (url) {
  const m = /^s3:\/\/([^/]+)(?:\/(.*))?$/.exec(url);
  if (!m) throw new TypeError(`Bad S3 URL: ${url}`)
  return { Bucket: m[1], Key: m[2] || '' }
}

function list$2 (baseurl) {
  if (!baseurl.endsWith('/')) baseurl = baseurl + '/';
  const { Bucket, Key: Prefix } = parseAddress(baseurl);
  const lister = new EventEmitter();
  lister.done = (async () => {
    const s3 = await getS3();
    const request = { Bucket, Prefix };
    while (true) {
      const result = await s3.listObjectsV2(request).promise();
      const files = result.Contents.map(item => {
        const file = {};
        file.url = `s3://${Bucket}/${item.Key}`;
        file.path = file.url.slice(baseurl.length);
        file.size = item.Size;
        file.mtime = item.LastModified;
        if (!item.ETag.includes('-')) file.md5 = item.ETag.replaceAll('"', '');
        file.storage = item.StorageClass;
        return file
      });
      if (files.length) lister.emit('files', files);
      if (!result.IsTruncated) break
      request.ContinuationToken = result.NextContinuationToken;
    }
    lister.emit('done');
  })();
  return lister
}

async function stat (url) {
  const { Bucket, Key } = parseAddress(url);
  const s3 = await getS3();

  const request = { Bucket, Key };
  const res = await s3.headObject(request).promise();
  const attrs = unpackMetadata(res.Metadata);
  return {
    contentType: res.ContentType,
    mtime: res.LastModified,
    size: res.ContentLength,
    md5: !res.ETag.includes('-') ? res.ETag.replaceAll('"', '') : attrs.md5,
    storage: res.StorageClass,
    attrs
  }
}

async function getHash$2 (url) {
  const { mtime, size, attrs } = await stat(url);
  const hash = attrs && attrs.md5 ? attrs.md5 : null;
  insertHash({ url, mtime, size, hash });
  return hash
}

async function createReadStream$1 (url) {
  const { Bucket, Key } = parseAddress(url);
  const s3 = await getS3();
  const { mtime, size, contentType, hash, attrs } = await stat(url);
  const source = { mtime, size, contentType, hash, attrs };

  const stream = s3.getObject({ Bucket, Key }).createReadStream();
  stream.source = source;
  return stream
}

async function createWriteStream$1 (url, source) {
  const { size, contentType, hash, attrs } = source;
  const { Bucket, Key } = parseAddress(url);
  const s3 = await getS3();
  const passthru = new PassThrough();
  if (!hash) throw new Error('No hash supplied')
  if (!size) throw new Error('No size supplied')
  if (!contentType) throw new Error('No contentType supplied')

  const request = {
    Body: passthru,
    Bucket,
    Key,
    ContentLength: size,
    ContentType: contentType,
    ContentMD5: Buffer.from(hash, 'hex').toString('base64'),
    Metadata: packMetadata(attrs)
  };

  passthru.done = s3
    .putObject(request)
    .promise()
    .then(() => getHash$2(url));
  return passthru
}

async function remove$2 (url) {
  const { Bucket, Key } = parseAddress(url);
  const s3 = await getS3();
  await s3.deleteObject({ Bucket, Key }).promise();
}

function unpackMetadata (md, key = 's3cmd-attrs') {
  const numbers = new Set(['mode', 'size', 'gid', 'uid']);
  const dates = new Set(['atime', 'mtime', 'ctime']);
  if (!md || typeof md !== 'object' || !md[key]) return {}
  return Object.fromEntries(
    md[key]
      .split('/')
      .map(x => x.split(':'))
      .map(([k, v]) => {
        if (dates.has(k)) {
          v = new Date(Number(v));
        } else if (numbers.has(k)) {
          v = Number(v);
        }
        return [k, v]
      })
  )
}

function packMetadata (obj, key = 's3cmd-attrs') {
  return {
    [key]: Object.keys(obj)
      .sort()
      .map(k => [k, obj[k]])
      .filter(([k, v]) => v != null)
      .map(([k, v]) => `${k}:${v instanceof Date ? +v : v}`)
      .join('/')
  }
}

async function hashFile (filename, { algo = 'md5', enc = 'hex' } = {}) {
  const hasher = createHash(algo);
  for await (const chunk of createReadStream$2(filename)) {
    hasher.update(chunk);
  }
  return hasher.digest(enc)
}

function list$1 (baseurl) {
  const lister = new EventEmitter$1();
  let basepath = baseurl.slice(7);
  lister.done = (async () => {
    basepath = await realpath(basepath);
    if (!basepath.endsWith('/')) basepath += '/';
    await scan(basepath);
  })();
  return lister

  async function scan (dir) {
    const entries = await readdir(dir, { withFileTypes: true });
    const files = await Promise.all(
      entries
        .filter(x => x.isFile())
        .map(async ({ name }) => {
          const fullname = dir + name;
          const path = fullname.slice(basepath.length);
          const url = 'file://' + fullname;
          const stats = await stat$1(fullname);
          return {
            url,
            path,
            size: stats.size,
            mtime: new Date(Math.round(stats.mtimeMs)),
            mode: stats.mode & 0o777
          }
        })
    );
    if (files.length) lister.emit('files', files);
    const dirs = entries
      .filter(x => x.isDirectory())
      .map(x => dir + x.name + '/');
    await Promise.all(dirs.map(scan));
  }
}

async function getHash$1 (url, stats) {
  const path = url.slice(7);
  if (!stats) stats = await stat$1(path);
  const { mtime, size } = stats;
  let hash = selectHash({ url, mtime, size });
  if (hash) return hash
  hash = await hashFile(path);
  insertHash({ url, mtime, size, hash });
  return hash
}

async function createReadStream (url) {
  const path = url.slice(7);
  const stats = await stat$1(path);
  const hash = await getHash$1(url, stats);
  const attrs = {
    atime: stats.atime,
    ctime: stats.ctime,
    mtime: stats.mtime,
    uid: 1000,
    gid: 1000,
    uname: 'alan',
    gname: 'alan',
    mode: stats.mode & 0o777,
    md5: hash
  };
  const source = {
    size: stats.size,
    mtime: stats.mtime,
    contentType: mime.getType(extname(path)),
    hash,
    attrs
  };

  const stream = createReadStream$2(path);
  stream.source = source;
  return stream
}

async function createWriteStream (url, source) {
  const path = url.slice(7);
  const { attrs } = source;
  const mtime = attrs && attrs.mtime;
  const mode = attrs && attrs.mode;
  const stream = createWriteStream$2(path);
  const streamComplete = new Promise((resolve, reject) =>
    stream.on('error', reject).on('finish', resolve)
  );
  stream.done = streamComplete.then(async () => {
    if (mode) await chmod(path, mode);
    if (mtime) await utimes(path, mtime, mtime);
    return await getHash$1(url)
  });
  return stream
}

async function remove$1 (url) {
  const path = url.slice(7);
  await unlink(path);
}

function list (url) {
  if (isS3(url)) return list$2(url)
  else if (isLocal(url)) return list$1(url)
  else throw new Error('Huh? ' + url)
}

async function copy (srcUrl, dstUrl, opts = {}) {
  const { onProgress, limit } = opts;
  const sourceStream = isS3(srcUrl)
    ? await createReadStream$1(srcUrl)
    : await createReadStream(srcUrl);

  const { source } = sourceStream;

  const destStream = isS3(dstUrl)
    ? await createWriteStream$1(dstUrl, source)
    : await createWriteStream(dstUrl, source);

  const speedo$1 = speedo({ total: source.size });

  const pPipeline = pipeline(
    [
      sourceStream,
      limit && throttle(limit),
      onProgress && speedo$1,
      onProgress && progressStream({ onProgress, speedo: speedo$1 }),
      destStream
    ].filter(Boolean)
  );

  await Promise.all([pPipeline, destStream.done]);
}

function getHash (url) {
  if (isS3(url)) return getHash$2(url)
  else if (isLocal(url)) return getHash$1(url)
  else throw new Error('Huh? ' + url)
}

function remove (url) {
  if (isS3(url)) return remove$2(url)
  else if (isLocal(url)) return remove$1(url)
  else throw new Error('Huh? ' + url)
}

function isS3 (url) {
  return url.startsWith('s3://')
}

function isLocal (url) {
  return url.startsWith('file:///')
}

var SEC = 1e3,
	MIN = SEC * 60,
	HOUR = MIN * 60,
	DAY = HOUR * 24,
	YEAR = DAY * 365.25;

function fmt(val, pfx, str, long) {
	var num = (val | 0) === val ? val : ~~(val + 0.5);
	return pfx + num + (long ? (' ' + str + (num != 1 ? 's' : '')) : str[0]);
}

function format(num, long) {
	var pfx = num < 0  ? '-' : '', abs = num < 0 ? -num : num;
	if (abs < SEC) return num + (long ? ' ms' : 'ms');
	if (abs < MIN) return fmt(abs / SEC, pfx, 'second', long);
	if (abs < HOUR) return fmt(abs / MIN, pfx, 'minute', long);
	if (abs < DAY) return fmt(abs / HOUR, pfx, 'hour', long);
	if (abs < YEAR) return fmt(abs / DAY, pfx, 'day', long);
	return fmt(abs / YEAR, pfx, 'year', long);
}

var RGX = /([^{]*?)\w(?=\})/g;

var MAP = {
	YYYY: 'getFullYear',
	YY: 'getYear',
	MM: function (d) {
		return d.getMonth() + 1;
	},
	DD: 'getDate',
	HH: 'getHours',
	mm: 'getMinutes',
	ss: 'getSeconds',
	fff: 'getMilliseconds'
};

function tinydate (str, custom) {
	var parts=[], offset=0;

	str.replace(RGX, function (key, _, idx) {
		// save preceding string
		parts.push(str.substring(offset, idx - 1));
		offset = idx += key.length + 1;
		// save function
		parts.push(custom && custom[key] || function (d) {
			return ('00' + (typeof MAP[key] === 'string' ? d[MAP[key]]() : MAP[key](d))).slice(-key.length);
		});
	});

	if (offset !== str.length) {
		parts.push(str.substring(offset));
	}

	return function (arg) {
		var out='', i=0, d=arg||new Date();
		for (; i<parts.length; i++) {
			out += (typeof parts[i]==='string') ? parts[i] : parts[i](d);
		}
		return out;
	};
}

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
  const msg = format$1(...args);
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

const reporter = new EventEmitter();
const { green, cyan } = log;

function report (msg, payload) {
  reporter.emit(msg, payload);
}

reporter
  .on('list.file', data => {
    let s = '';
    if (data.long) {
      s += data.storage.padEnd(1) + '  ';
      const size = data.human ? fmtSize(data.size) : data.size.toString();
      s += size.padStart(10) + '  ';
      s += fmtDate(data.mtime).padEnd(18) + '  ';
    }
    log(s + data.path);
  })
  .on('list.file.totals', ({ totalSize, totalCount, total, human }) => {
    if (!total) return
    const s = human ? `${fmtSize(totalSize)}B` : `${comma(totalSize)} bytes`;
    log(`\n${s} in ${comma(totalCount)} file${totalCount > 1 ? 's' : ''}`);
  })
  .on('cp', opts => opts.quiet || log(opts.url))
  .on('cp.start', url => log(cyan(url)))
  .on('cp.update', data => {
    const { bytes, percent, total, taken, eta, speed } = data;
    log.status(
      [
        comma(bytes).padStart(1 + comma(total).length),
        `${percent.toString().padStart(3)}%`,
        `time ${fmtTime(taken)}`,
        `eta ${fmtTime(eta)}`,
        `rate ${fmtSize(speed)}B/s`
      ].join(' ')
    );
  })
  .on('cp.done', ({ bytes, taken, speed }) => {
    log(
      green(
        [
          ` ${comma(bytes)} bytes copied`,
          `in ${fmtTime(taken)}`,
          `at ${fmtSize((bytes * 1e3) / taken)}B/s`
        ].join(' ')
      )
    );
  })
  .on('cp.dryrun', ({ url }) => log(`${url} - copied (dry run)`))
  .on('sync.scan.start', ({ kind }) => log.status(`Scanning ${kind} ... `))
  .on('sync.scan', ({ kind, count }) =>
    log.status(`Scanning ${kind} ... ${count}`)
  )
  .on('sync.scan.done', () => log.status(''))
  .on('sync.start', () => log.status('Scanning files'))
  .on('sync.hash', url => log.status(`${url} - hashing`))
  .on('sync.done', count =>
    log(`${comma(count)} file${count > 1 ? 's' : ''} processed.`)
  )
  .on('rm.dryrun', url => log(`${url} - deleted (dry run)`))
  .on('rm', url => log(`${url} - deleted`))
  .on('retry', ({ delay, error }) => {
    console.error(
      `\nError occured: ${error.message}\nWaiting ${fmtTime(delay)} to retry...`
    );
  });

function fmtTime (ms) {
  if (ms < 1000) ms = 1000 * Math.round(ms / 1000);
  if (!ms) return '0s'
  return format(ms)
}

function fmtSize (n) {
  const suffixes = [
    ['G', 1024 * 1024 * 1024],
    ['M', 1024 * 1024],
    ['K', 1024],
    ['', 1]
  ];

  for (const [suffix, factor] of suffixes) {
    if (n >= factor) {
      return (n / factor).toFixed(1) + suffix
    }
  }
  return '0'
}

function comma (n) {
  if (typeof n !== 'number') return ''
  return n.toLocaleString()
}

const fmtDate = tinydate('{DD}-{MMM}-{YY} {HH}:{mm}:{ss}', {
  MMM: d => d.toLocaleString(undefined, { month: 'short' }).slice(0, 3)
});

async function ls (url, options) {
  let totalCount = 0;
  let totalSize = 0;

  const lister = list(url);
  lister.on('files', files => {
    files.forEach(file => {
      totalCount++;
      totalSize += file.size || 0;
      file.storage = STORAGE_CLASS[file.storage] || 'F';
      report('list.file', { ...options, ...file });
    });
  });
  await lister.done;
  report('list.file.totals', { ...options, totalSize, totalCount });
}

const STORAGE_CLASS = {
  STANDARD: 'S',
  STANDARD_IA: 'I',
  GLACIER: 'G',
  DEEP_ARCHIVE: 'D'
};

function retry (fn, opts = {}) {
  return tryOne({ ...opts, fn, attempt: 1 })
}

function tryOne (options) {
  const {
    fn,
    attempt,
    retries = 10,
    delay = 1000,
    backoff = retry.exponential(1.5),
    onRetry
  } = options;
  return new Promise(resolve => resolve(fn())).catch(error => {
    if (attempt > retries) throw error
    if (onRetry) onRetry({ error, attempt, delay });
    return sleep(delay).then(() =>
      tryOne({ ...options, attempt: attempt + 1, delay: backoff(delay) })
    )
  })
}

retry.exponential = x => n => Math.round(n * x);

const sleep = delay => new Promise(resolve => setTimeout(resolve, delay));

const validProtocols = /^(?:s3|file):\/\//;

function validateUrl (url, { dir } = {}) {
  if (url.startsWith('/')) url = 'file://' + url;
  if (!validProtocols.test(url)) {
    throw new Error('Unknown type of URI: ' + url)
  }
  if (dir && !url.endsWith('/')) url += '/';
  return url
}

async function cp (fromUrl, toUrl, opts) {
  fromUrl = validateUrl(fromUrl);
  toUrl = validateUrl(toUrl);

  const { limit, progress, dryRun } = opts;
  if (dryRun) return report('cp.dryrun', { url: toUrl })

  const copyOpts = {
    limit,
    onProgress: progress ? doProgress(toUrl) : undefined
  };
  const retryOpts = {
    retries: 5,
    delay: 5000,
    onRetry: data => report('retry', data)
  };

  await retry(() => copy(fromUrl, toUrl, copyOpts), retryOpts);
  if (!progress) {
    report('cp', { url: toUrl, ...opts });
  }
}

function doProgress (url) {
  report('cp.start', url);
  return data => {
    const { bytes, done, speedo } = data;
    const { percent, total, taken, eta, rate: speed } = speedo;
    const payload = { bytes, percent, total, eta, speed, taken };
    report(`cp.${done ? 'done' : 'update'}`, payload);
  }
}

async function rm (url, opts) {
  url = validateUrl(url);
  const { dryRun } = opts;
  if (dryRun) return report('rm.dryrun', url)
  report('rm', url);
  await remove(url);
}

async function sync (srcRoot, dstRoot, opts) {
  srcRoot = validateUrl(srcRoot, { dir: true });
  dstRoot = validateUrl(dstRoot, { dir: true });

  await scanFiles(srcRoot, 'src', 'source');
  await scanFiles(dstRoot, 'dst', 'destination');
  report('sync.scan.done');

  for (const { url, path } of selectMissingFiles()) {
    await cp(url, dstRoot + path, { ...opts, progress: true });
  }

  for (const url of selectMissingHashes()) {
    report('sync.hash', url);
    await getHash(url);
  }

  for (const { from, to } of selectChanged()) {
    await cp(from, to, { ...opts, progress: true });
  }

  if (opts.delete) {
    for (const url of selectSurplusFiles()) {
      await rm(url, opts);
    }
  }
  report('sync.done', countFiles());
}

async function scanFiles (root, type, desc) {
  report('sync.scan.start', { kind: desc });
  let count = 0;
  const lister = list(root);
  lister.on('files', files => {
    count += files.length;
    report('sync.scan', { kind: desc, count });
    insertSyncFiles(type, files);
  });
  await lister.done;
}

const prog = sade('s3cli');
const version = '2.0.0';

prog.version(version);

prog
  .command('ls <url>', 'list the files under a dir')
  .option('-l, --long', 'show more detail')
  .option('-t, --total', 'include a total in long listing')
  .option('-H, --human', 'show human sizes in long listing')
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
