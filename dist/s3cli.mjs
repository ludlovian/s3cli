#!/usr/bin/env node
import sade from 'sade';
import { createReadStream, createWriteStream, unlinkSync } from 'fs';
import { stat as stat$2, chmod, utimes, lstat, readdir, symlink, readFile, open, rename, appendFile, realpath, unlink } from 'fs/promises';
import { PassThrough } from 'stream';
import { pipeline } from 'stream/promises';
import AWS from 'aws-sdk';
import { createHash } from 'crypto';
import mime from 'mime';
import { extname, resolve, join, basename, relative } from 'path';
import EventEmitter from 'events';
import { format as format$1 } from '@lukeed/ms';
import tinydate from 'tinydate';
import { format } from 'util';
import { cyan as cyan$1, green as green$1, yellow, blue, magenta, red } from 'kleur/colors';
import { homedir } from 'os';

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

function once$1 (fn) {
  let called = false;
  let value;
  return (...args) => {
    if (called) return value
    value = fn(...args);
    called = true;
    return value
  }
}

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

const getS3 = once$1(async () => {
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

const colourFuncs = { cyan: cyan$1, green: green$1, yellow, blue, magenta, red };
const colours = Object.keys(colourFuncs);
const CLEAR_LINE = '\r\x1b[0K';
const RE_DECOLOR = /(^|[^\x1b]*)((?:\x1b\[\d*m)|$)/g; // eslint-disable-line no-control-regex

const state = {
  dirty: false,
  width: process.stdout && process.stdout.columns,
  level: process.env.LOGLEVEL,
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
  if (colour && colour in colourFuncs) string = colourFuncs[colour](string);
  if (limitWidth) string = truncate(string, state.width);
  if (newline) string = string + '\n';
  if (state.dirty) string = CLEAR_LINE + string;
  state.dirty = !newline && !!msg;
  state.write(string);
}

function truncate (string, max) {
  max -= 2; // leave two chars at end
  if (string.length <= max) return string
  const parts = [];
  let w = 0
  ;[...string.matchAll(RE_DECOLOR)].forEach(([, txt, clr]) => {
    parts.push(txt.slice(0, max - w), clr);
    w = Math.min(w + txt.length, max);
  });
  return parts.join('')
}

function merge (old, new_) {
  const prefix = (old.prefix || '') + (new_.prefix || '');
  return { ...old, ...new_, prefix }
}

function logger (options) {
  return Object.defineProperties((...args) => _log(args, options), {
    _preset: { value: options, configurable: true },
    _state: { value: state, configurable: true },
    name: { value: 'log', configurable: true }
  })
}

function nextColour () {
  const clr = colours.shift();
  colours.push(clr);
  return clr
}

function fixup (log) {
  const p = log._preset;
  Object.assign(log, {
    status: logger(merge(p, { newline: false, limitWidth: true })),
    level: level => fixup(logger(merge(p, { level }))),
    colour: colour =>
      fixup(logger(merge(p, { colour: colour || nextColour() }))),
    prefix: prefix => fixup(logger(merge(p, { prefix }))),
    ...colourFuncs
  });
  return log
}

const log = fixup(logger({}));

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
          `time ${format$1(taken)}`,
          `eta ${eta < 1000 ? '0s' : format$1(eta)}`,
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
          `in ${format$1(taken, true)}`,
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
      `\nError occured: ${error.message}\nWaiting ${format$1(delay)} to retry...`
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

function uniq (arrs) {
  return [...new Set([].concat(...arrs))]
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

  const keys = uniq(streams.map(stream => stream.keys()));
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

class DatastoreError extends Error {
  constructor (name, message) {
    super(message);
    this.name = name;
    Error.captureStackTrace(this, this.constructor);
  }
}

class DatabaseLocked extends DatastoreError {
  constructor (filename) {
    super('DatabaseLocked', 'Database is locked');
    this.filename = filename;
  }
}

class KeyViolation extends DatastoreError {
  constructor (doc, fieldName) {
    super('KeyViolation', 'Key violation error');
    this.fieldName = fieldName;
    this.record = doc;
  }
}

class NotExists extends DatastoreError {
  constructor (doc) {
    super('NotExists', 'Record does not exist');
    this.record = doc;
  }
}

class NoIndex extends DatastoreError {
  constructor (fieldName) {
    super('NoIndex', 'No such index');
    this.fieldName = fieldName;
  }
}

class PLock {
  constructor ({ width = 1 } = {}) {
    this.width = width;
    this.count = 0;
    this.awaiters = [];
  }

  acquire () {
    if (this.count < this.width) {
      this.count++;
      return Promise.resolve()
    }
    return new Promise(resolve => this.awaiters.push(resolve))
  }

  release () {
    if (!this.count) return
    if (this.waiting) {
      this.awaiters.shift()();
    } else {
      this.count--;
    }
  }

  get waiting () {
    return this.awaiters.length
  }

  async exec (fn) {
    try {
      await this.acquire();
      return await Promise.resolve(fn())
    } finally {
      this.release();
    }
  }
}

function delve (obj, key) {
  let p = 0;
  key = key.split('.');
  while (obj && p < key.length) {
    obj = obj[key[p++]];
  }
  return obj === undefined || p < key.length ? undefined : obj
}

function getId (row, existing) {
  // generate a repeatable for this row, avoiding conflicts with the other rows
  const start = hashString(stringify(row));
  for (let n = 0; n < 1e8; n++) {
    const id = ((start + n) & 0x7fffffff).toString(36);
    if (!existing.has(id)) return id
  }
  /* c8 ignore next */
  throw new Error('Could not generate unique id')
}

function hashString (string) {
  return Array.from(string).reduce(
    (h, ch) => ((h << 5) - h + ch.charCodeAt(0)) & 0xffffffff,
    0
  )
}

function cleanObject (obj) {
  return Object.entries(obj).reduce((o, [k, v]) => {
    if (v !== undefined) o[k] = v;
    return o
  }, {})
}

const DATE = '$date';

function stringify (obj) {
  return JSON.stringify(obj, function (k, v) {
    return this[k] instanceof Date ? { [DATE]: this[k].toISOString() } : v
  })
}

function parse (s) {
  return JSON.parse(s, function (k, v) {
    if (k === DATE) return new Date(v)
    if (v && typeof v === 'object' && DATE in v) return v[DATE]
    return v
  })
}

function sortOn (selector) {
  if (typeof selector !== 'function') {
    const key = selector;
    selector = x => delve(x, key);
  }
  return (a, b) => {
    const x = selector(a);
    const y = selector(b);
    /* c8 ignore next */
    return x < y ? -1 : x > y ? 1 : 0
  }
}

// Indexes are maps between values and docs
//
// Generic index is many-to-many
// Unique index is many values to single doc
// Sparse indexes do not index null-ish values
//
class Index {
  static create (options) {
    return new (options.unique ? UniqueIndex : Index)(options)
  }

  constructor (options) {
    this.options = options;
    this.data = new Map();
  }

  find (value) {
    const docs = this.data.get(value);
    return docs ? Array.from(docs) : []
  }

  findOne (value) {
    const docs = this.data.get(value);
    return docs ? docs.values().next().value : undefined
  }

  addDoc (doc) {
    const value = delve(doc, this.options.fieldName);
    if (Array.isArray(value)) {
      value.forEach(v => this.linkValueToDoc(v, doc));
    } else {
      this.linkValueToDoc(value, doc);
    }
  }

  removeDoc (doc) {
    const value = delve(doc, this.options.fieldName);
    if (Array.isArray(value)) {
      value.forEach(v => this.unlinkValueFromDoc(v, doc));
    } else {
      this.unlinkValueFromDoc(value, doc);
    }
  }

  linkValueToDoc (value, doc) {
    if (value == null && this.options.sparse) return
    const docs = this.data.get(value);
    if (docs) {
      docs.add(doc);
    } else {
      this.data.set(value, new Set([doc]));
    }
  }

  unlinkValueFromDoc (value, doc) {
    const docs = this.data.get(value);
    if (!docs) return
    docs.delete(doc);
    if (!docs.size) this.data.delete(value);
  }
}

class UniqueIndex extends Index {
  findOne (value) {
    return this.data.get(value)
  }

  find (value) {
    return this.findOne(value)
  }

  linkValueToDoc (value, doc) {
    if (value == null && this.options.sparse) return
    if (this.data.has(value)) {
      throw new KeyViolation(doc, this.options.fieldName)
    }
    this.data.set(value, doc);
  }

  unlinkValueFromDoc (value, doc) {
    if (this.data.get(value) === doc) this.data.delete(value);
  }
}

const lockfiles = new Set();

async function lockFile (filename) {
  const lockfile = filename + '.lock~';
  const target = basename(filename);
  try {
    await symlink(target, lockfile);
    lockfiles.add(lockfile);
  } catch (err) {
    /* c8 ignore next */
    if (err.code !== 'EEXIST') throw err
    throw new DatabaseLocked(filename)
  }
}

function cleanup () {
  lockfiles.forEach(file => {
    try {
      unlinkSync(file);
    } catch {
      // pass
    }
  });
}

/* c8 ignore next 4 */
function cleanAndGo () {
  cleanup();
  setImmediate(() => process.exit(2));
}

process.on('exit', cleanup).on('SIGINT', cleanAndGo);

class Datastore {
  constructor (options) {
    this.options = {
      serialize: stringify,
      deserialize: parse,
      special: {
        deleted: '$$deleted',
        addIndex: '$$addIndex',
        deleteIndex: '$$deleteIndex'
      },
      ...options
    };

    this.lock = new PLock();
    this.lock.acquire();
    this.loaded = false;
    this.empty();
  }

  // API from Database class - mostly async

  async exec (fn) {
    const pItem = this.lock.exec(fn);
    if (!this.loaded) {
      this.loaded = true;
      await lockFile(this.options.filename);
      await this.hydrate();
      await this.rewrite();
      this.lock.release();
    }
    return await pItem
  }

  async ensureIndex (options) {
    const { fieldName } = options;
    const { addIndex } = this.options.special;
    if (this.hasIndex(fieldName)) return
    this.addIndex(options);
    await this.append([{ [addIndex]: options }]);
  }

  async deleteIndex (fieldName) {
    const { deleteIndex } = this.options.special;
    if (fieldName === '_id') return
    if (!this.hasIndex(fieldName)) throw new NoIndex(fieldName)
    this.removeIndex(fieldName);
    await this.append([{ [deleteIndex]: { fieldName } }]);
  }

  find (fieldName, value) {
    if (!this.hasIndex(fieldName)) throw new NoIndex(fieldName)
    return this.indexes[fieldName].find(value)
  }

  findOne (fieldName, value) {
    if (!this.hasIndex(fieldName)) throw new NoIndex(fieldName)
    return this.indexes[fieldName].findOne(value)
  }

  allDocs () {
    return Array.from(this.indexes._id.data.values())
  }

  async upsert (docOrDocs, options) {
    let ret;
    let docs;
    if (Array.isArray(docOrDocs)) {
      ret = docOrDocs.map(doc => this.addDoc(doc, options));
      docs = ret;
    } else {
      ret = this.addDoc(docOrDocs, options);
      docs = [ret];
    }
    await this.append(docs);
    return ret
  }

  async delete (docOrDocs) {
    let ret;
    let docs;
    const { deleted } = this.options.special;
    if (Array.isArray(docOrDocs)) {
      ret = docOrDocs.map(doc => this.removeDoc(doc));
      docs = ret;
    } else {
      ret = this.removeDoc(docOrDocs);
      docs = [ret];
    }
    docs = docs.map(doc => ({ [deleted]: doc }));
    await this.append(docs);
    return ret
  }

  async hydrate () {
    const {
      filename,
      deserialize,
      special: { deleted, addIndex, deleteIndex }
    } = this.options;

    const data = await readFile(filename, { encoding: 'utf8', flag: 'a+' });

    this.empty();
    for (const line of data.split(/\n/).filter(Boolean)) {
      const doc = deserialize(line);
      if (addIndex in doc) {
        this.addIndex(doc[addIndex]);
      } else if (deleteIndex in doc) {
        this.deleteIndex(doc[deleteIndex].fieldName);
      } else if (deleted in doc) {
        this.removeDoc(doc[deleted]);
      } else {
        this.addDoc(doc);
      }
    }
  }

  async rewrite ({ sorted = false, sortBy } = {}) {
    const {
      filename,
      serialize,
      special: { addIndex }
    } = this.options;
    const temp = filename + '~';
    const docs = this.allDocs();
    if (sorted) {
      if (typeof sorted !== 'string' && typeof sorted !== 'function') {
        sorted = '_id';
      }
      sortBy = sortOn(sorted);
    }
    if (sortBy && typeof sortBy === 'function') {
      docs.sort(sortBy);
    }
    const lines = Object.values(this.indexes)
      .filter(ix => ix.options.fieldName !== '_id')
      .map(ix => ({ [addIndex]: ix.options }))
      .concat(docs)
      .map(doc => serialize(doc) + '\n');
    const fh = await open(temp, 'w');
    await fh.writeFile(lines.join(''), 'utf8');
    await fh.sync();
    await fh.close();
    await rename(temp, filename);
  }

  async append (docs) {
    const { filename, serialize } = this.options;
    const lines = docs.map(doc => serialize(doc) + '\n').join('');
    await appendFile(filename, lines, 'utf8');
  }

  // Internal methods - mostly sync

  empty () {
    this.indexes = {
      _id: Index.create({ fieldName: '_id', unique: true })
    };
  }

  addIndex (options) {
    const { fieldName } = options;
    const ix = Index.create(options);
    this.allDocs().forEach(doc => ix.addDoc(doc));
    this.indexes[fieldName] = ix;
  }

  removeIndex (fieldName) {
    delete this.indexes[fieldName];
  }

  hasIndex (fieldName) {
    return Boolean(this.indexes[fieldName])
  }

  addDoc (doc, { mustExist = false, mustNotExist = false } = {}) {
    const { _id, ...rest } = doc;
    const olddoc = this.indexes._id.findOne(_id);
    if (!olddoc && mustExist) throw new NotExists(doc)
    if (olddoc && mustNotExist) throw new KeyViolation(doc, '_id')

    doc = {
      _id: _id || getId(doc, this.indexes._id.data),
      ...cleanObject(rest)
    };
    Object.freeze(doc);

    const ixs = Object.values(this.indexes);
    try {
      ixs.forEach(ix => {
        if (olddoc) ix.removeDoc(olddoc);
        ix.addDoc(doc);
      });
      return doc
    } catch (err) {
      // to rollback, we remove the new doc from each index. If there is
      // an old one, then we remove that (just in case) and re-add
      ixs.forEach(ix => {
        ix.removeDoc(doc);
        if (olddoc) {
          ix.removeDoc(olddoc);
          ix.addDoc(olddoc);
        }
      });
      throw err
    }
  }

  removeDoc (doc) {
    const ixs = Object.values(this.indexes);
    const olddoc = this.indexes._id.findOne(doc._id);
    if (!olddoc) throw new NotExists(doc)
    ixs.forEach(ix => ix.removeDoc(olddoc));
    return olddoc
  }
}

// Database
//
// The public API of a jsdb database
//
class Database$1 {
  constructor (filename) {
    if (!filename || typeof filename !== 'string') {
      throw new TypeError('Bad filename')
    }
    filename = resolve(join(homedir(), '.databases'), filename);
    const ds = new Datastore({ filename });
    Object.defineProperties(this, {
      _ds: { value: ds, configurable: true },
      _autoCompaction: { configurable: true, writable: true }
    });
  }

  load () {
    return this.reload()
  }

  reload () {
    return this._ds.exec(() => this._ds.hydrate())
  }

  compact (opts) {
    return this._ds.exec(() => this._ds.rewrite(opts))
  }

  ensureIndex (options) {
    return this._ds.exec(() => this._ds.ensureIndex(options))
  }

  deleteIndex (fieldName) {
    return this._ds.exec(() => this._ds.deleteIndex(fieldName))
  }

  insert (docOrDocs) {
    return this._ds.exec(() =>
      this._ds.upsert(docOrDocs, { mustNotExist: true })
    )
  }

  update (docOrDocs) {
    return this._ds.exec(() => this._ds.upsert(docOrDocs, { mustExist: true }))
  }

  upsert (docOrDocs) {
    return this._ds.exec(() => this._ds.upsert(docOrDocs))
  }

  delete (docOrDocs) {
    return this._ds.exec(() => this._ds.delete(docOrDocs))
  }

  getAll () {
    return this._ds.exec(async () => this._ds.allDocs())
  }

  find (fieldName, value) {
    return this._ds.exec(async () => this._ds.find(fieldName, value))
  }

  findOne (fieldName, value) {
    return this._ds.exec(async () => this._ds.findOne(fieldName, value))
  }

  setAutoCompaction (interval, opts) {
    this.stopAutoCompaction();
    this._autoCompaction = setInterval(() => this.compact(opts), interval);
  }

  stopAutoCompaction () {
    if (!this._autoCompaction) return
    clearInterval(this._autoCompaction);
    this._autoCompaction = undefined;
  }
}

Object.assign(Database$1, { KeyViolation, NotExists, NoIndex, DatabaseLocked });

const once = fn => {
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
};

function sortBy (name, desc) {
  const fn = typeof name === 'function' ? name : x => x[name];
  const parent = typeof this === 'function' ? this : null;
  const m = desc ? -1 : 1;
  sortFunc.thenBy = sortBy;
  return sortFunc

  function sortFunc (a, b) {
    return (parent && parent(a, b)) || m * compare(a, b, fn)
  }

  function compare (a, b, fn) {
    const va = fn(a);
    const vb = fn(b);
    return va < vb ? -1 : va > vb ? 1 : 0
  }
}

class Database {
  constructor () {
    this.db = new Database$1('url_md5_cache.db');
  }

  async prepare () {
    await this.db.ensureIndex({ fieldName: 'url', unique: true });
  }

  async * rows (prefix, filter) {
    const urlPrefix = new URL(prefix);
    const rows = (await this.db.getAll())
      .filter(({ url }) => url.startsWith(urlPrefix.href))
      .sort(sortBy('url'));
    for (const row of rows) {
      const urlRow = new URL(row.url);
      const path = relative(urlPrefix.pathname, urlRow.pathname);
      if (!filter(path)) continue
      yield { ...row, path };
    }
  }

  async store (data) {
    const { _id, url, ...rest } = data;
    const row = await this.db.findOne('url', url);
    if (row) {
      await this.db.update({ ...row, ...rest });
    } else {
      await this.db.insert({ url, ...rest });
    }
  }

  async remove ({ url }) {
    const row = await this.db.findOne('url', url);
    if (row) await this.db.delete(row);
  }

  async compact () {
    await this.db.compact({ sorted: 'url' });
  }
}

const getDB = once(async function getDB () {
  const db = new Database();
  await db.prepare();
  return db
});

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
const version = '1.7.1';

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
