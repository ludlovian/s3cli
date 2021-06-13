#!/usr/bin/env node
import sade from 'sade';
import { createReadStream, createWriteStream } from 'fs';
import { stat as stat$2, chmod, utimes, lstat, readdir, realpath, unlink } from 'fs/promises';
import { PassThrough } from 'stream';
import { pipeline } from 'stream/promises';
import AWS from 'aws-sdk';
import { createHash } from 'crypto';
import mime from 'mime/lite.js';
import { extname, resolve, join, relative } from 'path';
import EventEmitter from 'events';
import { format as format$1 } from 'util';
import { homedir } from 'os';
import SQLite3 from 'better-sqlite3';

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

async function hashFile (filename, { algo = 'md5', enc = 'hex' } = {}) {
  const hasher = createHash(algo);
  for await (const chunk of createReadStream(filename)) {
    hasher.update(chunk);
  }
  return hasher.digest(enc)
}

async function getFileMetadata (file) {
  const { mtimeMs, ctimeMs, atimeMs, size, mode } = await stat$2(file);
  const md5 = await getLocalHash(file);
  const contentType = mime.getType(extname(file));
  const uid = 1000;
  const gid = 1000;
  const uname = 'alan';
  const gname = 'alan';
  return {
    uid,
    uname,
    gid,
    gname,
    atime: Math.floor(atimeMs),
    mtime: Math.floor(mtimeMs),
    ctime: Math.floor(ctimeMs),
    size,
    mode,
    md5,
    contentType
  }
}

const getLocalHash = hashFile;

function unpackMetadata (md, key = 's3cmd-attrs') {
  /* c8 ignore next */
  if (!md || typeof md !== 'object' || !md[key]) return {}
  return md[key].split('/').reduce((o, item) => {
    const [k, v] = item.split(':');
    o[k] = maybeNumber(v);
    return o
  }, {})
}

function packMetadata (obj, key = 's3cmd-attrs') {
  return {
    [key]: Object.keys(obj)
      .sort()
      .filter(k => obj[k] != null)
      .map(k => `${k}:${obj[k]}`)
      .join('/')
  }
}

function maybeNumber (v) {
  const n = parseInt(v, 10);
  if (!isNaN(n) && n.toString() === v) return n
  return v
}

const getS3 = once(async () => {
  const REGION = 'eu-west-1';
  return new AWS.S3({ region: REGION })
});

// parseAddress
//
// split an s3 url into Bucket and Key
//
function parseAddress (url) {
  url = new URL(url);
  const { protocol, hostname, pathname } = url;
  if (protocol !== 's3:') throw new TypeError(`Bad S3 URL: ${url}`)
  return { Bucket: hostname, Key: pathname.replace(/^\//, '') }
}

// scan
//
// List the objects in a bucket in an async generator
//
async function * scan (url, opts = {}) {
  const { Delimiter, MaxKeys } = opts;
  const { Bucket, Key: Prefix } = parseAddress(url);
  const s3 = await getS3();
  const request = { Bucket, Prefix, Delimiter, MaxKeys };

  let pResult = s3.listObjectsV2(request).promise();

  while (pResult) {
    const result = await pResult;
    // start the next one going if needed
    /* c8 ignore next 4 */
    if (result.IsTruncated) {
      request.ContinuationToken = result.NextContinuationToken;
      pResult = s3.listObjectsV2(request).promise();
    } else {
      pResult = null;
    }

    for (const item of result.Contents) {
      yield item;
    }

    /* c8 ignore next */
    for (const item of result.CommonPrefixes || []) {
      yield item;
    }
  }
}

// stat
//
// Perform a stat-like inspection of an object.
// Decode the s3cmd-attrs if given

async function stat$1 (url) {
  const { Bucket, Key } = parseAddress(url);
  const s3 = await getS3();

  const request = { Bucket, Key };
  const result = await s3.headObject(request).promise();
  return {
    ...result,
    ...unpackMetadata(result.Metadata)
  }
}

// upload
//
// uploads a file to S3 with progress and/or rate limiting

async function upload$1 (file, url, opts = {}) {
  const { Bucket, Key } = parseAddress(url);
  const { onProgress, interval = 1000, limit } = opts;

  const s3 = await getS3();
  const {
    size: ContentLength,
    contentType: ContentType,
    ...metadata
  } = await getFileMetadata(file);

  // streams
  const speedo$1 = speedo({ total: ContentLength });
  const Body = new PassThrough();

  const pPipeline = pipeline(
    ...[
      createReadStream(file),
      limit && throttle(limit),
      onProgress && speedo$1,
      onProgress && progressStream({ onProgress, interval, speedo: speedo$1 }),
      Body
    ].filter(Boolean)
  );

  const request = {
    Body,
    Bucket,
    Key,
    ContentLength,
    ContentType,
    ContentMD5: Buffer.from(metadata.md5, 'hex').toString('base64'),
    Metadata: packMetadata(metadata)
  };

  // perform the upload
  const pUpload = s3.putObject(request).promise();

  // wait for everything to finish
  await Promise.all([pPipeline, pUpload]);
  const { ETag } = await pUpload;

  // check the etag is the md5 of the source data
  /* c8 ignore next 3 */
  if (ETag !== `"${metadata.md5}"`) {
    throw new Error(`Upload of ${file} to ${url} failed`)
  }
}

// download
//
// download an S3 object to a file, with progress and/or rate limiting
//
async function download$1 (url, dest, opts = {}) {
  const { onProgress, interval = 1000, limit } = opts;
  const { Bucket, Key } = parseAddress(url);

  const s3 = await getS3();
  const { ETag, ContentLength: total, atime, mtime, mode, md5 } = await stat$1(
    url
  );
  /* c8 ignore next */
  const hash = md5 || (!ETag.includes('-') && ETag.replace(/"/g, ''));

  const hasher = hashStream();
  const speedo$1 = speedo({ total });
  const streams = [
    s3.getObject({ Bucket, Key }).createReadStream(),
    hasher,
    limit && throttle(limit),
    onProgress && speedo$1,
    onProgress && progressStream({ onProgress, interval, speedo: speedo$1 }),
    createWriteStream(dest)
  ].filter(Boolean);

  await pipeline(...streams);
  /* c8 ignore next 3 */
  if (hash && hash !== hasher.hash) {
    throw new Error(`Error downloading ${url} to ${dest}`)
  }

  if (mode) await chmod(dest, mode & 0o777);
  if (mtime && atime) await utimes(dest, new Date(atime), new Date(mtime));
}

async function deleteObject (url, opts = {}) {
  const { Bucket, Key } = parseAddress(url);
  const s3 = await getS3();

  const request = { Bucket, Key, ...opts };
  await s3.deleteObject(request).promise();
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
    if (data.long) {
      let type;
      let size = '';
      let time = '';

      if (data.directory) {
        type = 'D';
      } else {
        type = data.storageClass;
        size = data.human ? fmtSize(data.size) : data.size.toString();
        if (data.mtime) time = fmtDate(data.mtime);
      }
      log(
        [type.padEnd(1), size.padStart(10), time.padEnd(18), data.key].join(
          '  '
        )
      );
    } else {
      log(data.key);
    }
  })
  .on('list.file.totals', ({ totalSize, totalCount, total, human }) => {
    if (!total) return
    const s = human ? `${fmtSize(totalSize)}B` : `${comma(totalSize)} bytes`;
    log(`\n${s} in ${comma(totalCount)} file${totalCount > 1 ? 's' : ''}`);
  })
  .on('file.transfer.start', url => log(cyan(url)))
  .on(
    'file.transfer.update',
    ({ bytes, percent, total, taken, eta, speed }) => {
      log.status(
        [
          comma(bytes).padStart(1 + comma(total).length),
          `${percent.toString().padStart(3)}%`,
          `time ${format(taken)}`,
          `eta ${eta < 1000 ? '0s' : format(eta)}`,
          `rate ${fmtSize(speed)}B/s`
        ].join(' ')
      );
    }
  )
  .on('file.transfer.done', ({ bytes, taken, speed, direction }) => {
    log(
      green(
        [
          ` ${comma(bytes)} bytes`,
          direction,
          `in ${format(taken, true)}`,
          `at ${fmtSize((bytes * 1e3) / taken)}B/s`
        ].join(' ')
      )
    );
  })
  .on('sync.start', () => log.status('Scanning files'))
  .on('sync.file.start', path => log.status(path))
  .on('sync.file.hashing', path => log.status(`${path} - hashing`))
  .on('sync.file.dryrun', ({ path, action }) =>
    log(`${path} - ${action} (dry run)`)
  )
  .on('sync.done', ({ count }) =>
    log(`${comma(count)} file${count > 1 ? 's' : ''} processed.`)
  )
  .on('delete.file.start', path => log.status(`${path} - deleting `))
  .on('delete.file.done', path => log(`${path} - deleted`))
  .on('retry', ({ delay, error }) => {
    console.error(
      `\nError occured: ${error.message}\nWaiting ${format(delay)} to retry...`
    );
  })
  .on('stat.start', url => log(url + '\n'))
  .on('stat.details', ({ key, value, width }) =>
    log(
      [
        green(`${key}:`.padEnd(width + 2)),
        value instanceof Date ? fmtDate(value) : value
      ].join('')
    )
  );

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
  MMM: d => d.toLocaleString(undefined, { month: 'short' })
});

async function ls (url, options) {
  const { directory } = options;
  if (directory && !url.endsWith('/')) url += '/';

  let totalCount = 0;
  let totalSize = 0;

  const fileStream = scan(url, {
    Delimiter: directory ? '/' : undefined
  });

  for await (const {
    Key,
    Prefix,
    Size,
    LastModified,
    StorageClass
  } of fileStream) {
    if (Key && Key.endsWith('/')) continue

    totalCount++;
    totalSize += Size || 0;

    const storageClass = STORAGE_CLASS[StorageClass] || '?';
    const key = Prefix || Key;
    const mtime = LastModified;
    const size = Size;

    report('list.file', { ...options, key, mtime, size, storageClass });
  }
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

function upload (file, url, { progress, limit }) {
  const retryOpts = {
    retries: 5,
    delay: 5000,
    onRetry: data => report('retry', data)
  };
  const s3opts = { onProgress: !!progress && doProgress$1(url), limit };
  return retry(() => upload$1(file, url, s3opts), retryOpts)
}

function doProgress$1 (url) {
  report('file.transfer.start', url);
  const direction = 'uploaded';
  return data => {
    const { bytes, done, speedo } = data;
    const { percent, total, taken, eta, rate: speed } = speedo;
    const payload = { bytes, percent, total, taken, eta, speed, direction };
    report(`file.transfer.${done ? 'done' : 'update'}`, payload);
  }
}

function download (url, file, { progress, limit }) {
  const retryOpts = {
    retries: 5,
    delay: 5000,
    onRetry: data => report('retry', data)
  };
  const s3opts = { onProgress: !!progress && doProgress(url), limit };
  return retry(() => download$1(url, file, s3opts), retryOpts)
}

function doProgress (dest) {
  report('file.transfer.start', resolve(dest));
  const direction = 'downloaded';
  return data => {
    const { bytes, done, speedo } = data;
    const { total, percent, eta, taken, rate: speed } = speedo;
    const payload = { bytes, percent, total, taken, eta, speed, direction };
    report(`file.transfer.${done ? 'done' : 'update'}`, payload);
  }
}

function uniq (...values) {
  return [...new Set([].concat(...values))]
}

class Stream {
  constructor (source, selector) {
    Object.assign(this, {
      source,
      selector,
      done: false,
      value: undefined,
      key: undefined,
      missed: new Map()
    });
  }

  async read () {
    if (this.done) return
    const { done, value } = await this.source.next();
    if (done) {
      this.done = true;
      this.value = this.key = null;
    } else {
      this.value = value;
      this.key = this.selector(value);
    }
    return this.value
  }

  async readIfOn (key, store) {
    if (this.key !== key) return this.value
    if (store) this.missed.set(this.key, this.value);
    return this.read()
  }

  has (key) {
    // do we have this, either missed or current
    return this.key === key || this.missed.has(key)
  }

  get (key) {
    // return the item (current or missed)
    if (this.key === key) return this.value
    const value = this.missed.get(key);
    this.missed.delete(key);
    return value || null
  }

  keys () {
    return [...this.missed.keys()]
  }
}

function makeSelector (key) {
  return typeof key === 'function' ? key : record => record[key]
}

function allSame (vals) {
  for (let i = 1; i < vals.length; i++) {
    if (vals[i] !== vals[0]) return false
  }
  return true
}

function earliest (vals) {
  let ret;
  for (let i = 0; i < vals.length; i++) {
    if (!ret || vals[i] < ret) ret = vals[i];
  }
  return ret
}

async function * weave (keyFunc, ...sources) {
  if (!sources.length) return
  const selector = makeSelector(keyFunc);
  const streams = sources.map(source => new Stream(source, selector));

  await read(streams);
  while (!streams.every(stream => stream.done)) {
    const keys = streams.map(stream => stream.key);
    if (allSame(keys)) {
      const key = keys[0];
      yield valueFor(streams, key);
      await read(streams);
    } else {
      const key = earliest(keys);
      if (streams.every(stream => stream.has(key))) {
        yield valueFor(streams, key);
        await read(streams, key);
      } else {
        await read(streams, key, true);
      }
    }
  }

  const keys = uniq(...streams.map(stream => stream.keys()));
  for (const key of keys) {
    yield valueFor(streams, key);
  }
}

function read (streams, key, store) {
  if (key) {
    return Promise.all(streams.map(stream => stream.readIfOn(key, store)))
  } else {
    return Promise.all(streams.map(stream => stream.read()))
  }
}

function valueFor (streams, key) {
  return [key, ...streams.map(stream => stream.get(key))]
}

async function * filescan (options) {
  if (typeof options === 'string') options = { path: options };
  let { path: root, prune, depth } = options;
  if (!prune) prune = [];
  if (!Array.isArray(prune)) prune = [prune];
  prune = new Set(prune.map(path => join(root, path)));

  yield * scan(root);

  async function * scan (path) {
    if (prune.has(path)) return
    const stats = await lstat(path);
    if (!stats.isDirectory()) {
      yield { path, stats };
      return
    }
    let files = await readdir(path);
    files.sort();
    if (!depth) yield { path, stats, files };
    for (const file of files) {
      yield * scan(join(path, file));
    }
    if (depth) {
      files = await readdir(path);
      files.sort();
      yield { path, stats, files };
    }
  }
}

const db = new SQLite3(resolve(homedir(), '.databases', 'files.sqlite'));

const selectLocal = db.prepare(
  "SELECT 'file://' || path AS url, mtime, size, hash " +
    'FROM local_files ORDER BY url ASC;'
);
const selectS3 = db.prepare(
  "SELECT 's3://' || bucket || '/' || path AS url, mtime, size, hash " +
    'FROM s3_files ORDER BY url ASC;'
);

function * rows (prefix, filter) {
  if (!prefix.endsWith('/')) prefix += '/';
  let select;
  if (prefix.startsWith('file://')) select = selectLocal;
  else if (prefix.startsWith('s3://')) select = selectS3;
  else select = { all: () => [] };
  for (const row of select.all()) {
    if (!row.url.startsWith(prefix)) continue
    const path = row.url.slice(prefix.length);
    if (!filter(path)) continue
    yield { ...row, path };
  }
}

const replaceLocal = db.prepare(
  'REPLACE INTO local_files(path, mtime, size, hash) ' +
    'VALUES($path, $mtime, $size, $hash);'
);
const replaceS3 = db.prepare(
  'REPLACE INTO s3_files(bucket, path, mtime, size, hash) ' +
    'VALUES($bucket, $path, $mtime, $size, $hash);'
);

function store ({ url, mtime, size, hash }) {
  if (url.startsWith('file://')) {
    const path = url.slice(7);
    replaceLocal.run({ path, mtime, size, hash });
  } else if (url.startsWith('s3://')) {
    const u = new URL(url);
    const bucket = u.hostname;
    const path = u.pathname.replace(/^\//, '');
    replaceS3.run({ bucket, path, mtime, size, hash });
  }
}

const deleteLocal = db.prepare('DELETE FROM local_files WHERE path = $path;');
const deleteS3 = db.prepare(
  'DELETE FROM s3_files WHERE bucket = $bucket AND path = $path;'
);
function remove ({ url }) {
  if (url.startsWith('file://')) {
    const path = url.slice(7);
    deleteLocal.run({ path });
  } else if (url.startsWith('s3://')) {
    const u = new URL(url);
    const bucket = u.hostname;
    const path = u.pathname.replace(/^\//, '');
    deleteS3.run({ bucket, path });
  }
}

function compact () {}

async function getDB () {
  return { rows, store, remove, compact }
}

class Local extends EventEmitter {
  constructor (data) {
    super();
    Object.assign(this, data);
  }

  static async * files (root, filter) {
    for await (const { path: fullpath, stats } of filescan(root)) {
      if (!stats.isFile()) continue
      const path = relative(root, fullpath);
      if (!filter(path)) continue
      yield new Local({ path, fullpath, root, stats });
    }
  }

  static async * hashes (root, filter) {
    const db = await getDB();
    yield * db.rows('file://' + root, filter);
  }

  async getHash (row) {
    const stats = this.stats;
    if (row && stats.mtimeMs === row.mtime && stats.size === row.size) {
      this.hash = row.hash;
      return
    }

    this.emit('hashing');
    this.hash = await hashFile(this.fullpath);

    const db = await getDB();
    await db.store({
      url: `file://${join(this.root, this.path)}`,
      mtime: this.stats.mtimeMs,
      size: this.stats.size,
      hash: this.hash
    });
  }
}

class Remote extends EventEmitter {
  constructor (data) {
    super();
    Object.assign(this, data);
  }

  static async * files (root, filter) {
    const { Bucket, Key: Prefix } = parseAddress(root);
    for await (const data of scan(root + '/')) {
      const path = relative(Prefix, data.Key);
      if (data.Key.endsWith('/') || !filter(path)) continue
      yield new Remote({
        path,
        root,
        url: `s3://${Bucket}/${data.Key}`,
        mtime: +data.LastModified,
        size: data.Size
      });
    }
  }

  static async * hashes (root, filter) {
    const db = await getDB();
    yield * db.rows(root, filter);
  }

  async getHash (row) {
    if (row && row.mtime === this.mtime && row.size === this.size) {
      this.hash = row.hash;
      return
    }

    this.emit('hashing');
    const stats = await stat$1(this.url);
    this.hash = stats.md5 || 'UNKNOWN';

    const db = await getDB();
    await db.store({
      url: this.url,
      mtime: this.mtime,
      size: this.size,
      hash: this.hash
    });
  }
}

async function rm (url) {
  report('delete.file.start', url);
  await deleteObject(url);
  report('delete.file.done', url);
}

async function sync (
  lRoot,
  rRoot,
  { dryRun, download: downsync, delete: deleteExtra, ...options }
) {
  report('sync.start');
  lRoot = await realpath(resolve(lRoot.replace(/\/$/, '')));
  rRoot = rRoot.replace(/\/$/, '');

  const filter = getFilter(options);
  const lFiles = Local.files(lRoot, filter);
  const lHashes = Local.hashes(lRoot, filter);
  const rFiles = Remote.files(rRoot, filter);
  const rHashes = Remote.hashes(rRoot, filter);

  let fileCount = 0;
  const items = weave('path', lFiles, lHashes, rFiles, rHashes);

  for await (const item of items) {
    fileCount++;
    const [path, local, lrow, remote, rrow] = item;
    if (path) report('sync.file.start', path);

    if (local) {
      local.on('hashing', () => report('sync.file.hashing', path));
      await local.getHash(lrow);
    }

    if (remote) {
      remote.on('hashing', () => report('sync.file.hashing', path));
      await remote.getHash(rrow);
    }

    if (local && remote) {
      if (local.hash === remote.hash) continue
      if (downsync) {
        await downloadFile(remote.url, lRoot, path);
      } else {
        await uploadFile(local.fullpath, rRoot, path);
      }
    } else if (local) {
      if (downsync) {
        if (deleteExtra) {
          await deleteLocal(lRoot, path);
        }
      } else {
        await uploadFile(local.fullpath, rRoot, path);
      }
    } else if (remote) {
      if (downsync) {
        await downloadFile(remote.url, lRoot, path);
      } else {
        if (deleteExtra) {
          await deleteRemote(rRoot, path);
        }
      }
    } else {
      const db = await getDB();

      if (lrow) await db.remove(lrow);
      if (rrow) await db.remove(rrow);
    }
  }
  await getDB().then(db => db.compact());

  report('sync.done', { count: fileCount });

  async function uploadFile (file, rRoot, path, action = 'upload') {
    if (dryRun) return report('sync.file.dryrun', { path, action })
    return upload(file, `${rRoot}/${path}`, { ...options, progress: true })
  }

  async function downloadFile (url, lRoot, path, action = 'download') {
    if (dryRun) return report('sync.file.dryrun', { path, action })
    return download(url, join(lRoot, path), { ...options, progress: true })
  }

  async function deleteLocal (lRoot, path, action = 'delete') {
    if (dryRun) return report('sync.file.dryrun', { path, action })
    return unlink(join(lRoot, path))
  }

  async function deleteRemote (rRoot, path, action = 'delete') {
    if (dryRun) return report('sync.file.dryrun', { path, action })
    return rm(`${rRoot}/${path}`)
  }
}

function getFilter ({ filter }) {
  if (!filter) return () => true
  const rgx = new RegExp(filter);
  return x => rgx.test(x)
}

async function stat (url) {
  const data = await stat$1(url);
  const results = [];
  for (let [k, v] of Object.entries(data)) {
    k = k.charAt(0).toLowerCase() + k.slice(1);
    if (k === 'metadata') continue
    if (k.endsWith('time')) v = new Date(v * 1000);
    if (k === 'mode') v = '0o' + v.toString(8);
    results.push([k, v]);
  }
  results.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  const width = Math.max(...results.map(x => x[0].length));
  report('stat.start', url);
  for (const [key, value] of results) {
    report('stat.details', { key, value, width });
  }
  report('stat.done', url);
}

const prog = sade('s3cli');
const version = '1.8.2';

prog.version(version);

prog
  .command('ls <s3url>', 'list the objects in a bucket')
  .option('-l, --long', 'show more detail')
  .option('-t, --total', 'include a total in long listing')
  .option('-H, --human', 'show human sizes in long listing')
  .option('-d, --directory', 'list directories without recursing')
  .action(ls);

prog
  .command('upload <file> <s3url>', 'upload a file to S3')
  .option('-p, --progress', 'show progress')
  .option('-l, --limit', 'limit rate')
  .action(upload);

prog
  .command('download <s3url> <file>', 'download a file from S3')
  .option('-p, --progress', 'show progress')
  .option('-l, --limit', 'limit rate')
  .action(download);

prog
  .command('sync <dir> <s3url>', 'sync a directory with S3')
  .option('-p, --progress', 'show progress')
  .option('-l, --limit', 'limit rate')
  .option('-f, --filter', 'regex to limit the files synced')
  .option('-n, --dry-run', 'show what would be done')
  .option('-d, --delete', 'delete extra files on the destination')
  .option('-D, --download', 'sync from S3 down to local')
  .action(sync);

prog
  .command('stat <s3url>')
  .describe('show details about a file')
  .action(stat);

prog
  .command('rm <s3url>')
  .describe('delete a remote file')
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
