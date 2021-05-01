#!/usr/bin/env node
import sade from 'sade';
import { createReadStream, createWriteStream } from 'fs';
import { stat as stat$2, chmod, utimes, lstat, readdir, realpath, unlink } from 'fs/promises';
import { PassThrough } from 'stream';
import { pipeline } from 'stream/promises';
import AWS from 'aws-sdk';
import assert from 'assert/strict';
import crypto, { createHash } from 'crypto';
import mime from 'mime';
import { extname, resolve, join, relative } from 'path';
import EventEmitter from 'events';
import { format as format$1 } from '@lukeed/ms';
import tinydate from 'tinydate';
import { format } from 'util';
import { cyan as cyan$1, green as green$1, yellow, blue, magenta, red } from 'kleur/colors';
import { request } from 'http';
import 'net';

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
          assert.equal(data.length, 0);
          yield chunk;
          continue
        }
        bytes += chunkSize;
        assert.equal(chunkBytes, chunkSize);
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

async function hashFile$1 (filename, { algo = 'md5', enc = 'hex' } = {}) {
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

const getLocalHash = hashFile$1;

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
  const match = /^s3:\/\/([a-zA-Z0-9_-]+)\/?(.*)$/.exec(url);
  if (!match) throw new Error(`Bad S3 URL: ${url}`)
  const [, Bucket, Key] = match;
  return { Bucket, Key }
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

function upload (file, url, { progress, limit }) {
  return upload$1(file, url, {
    onProgress: !!progress && doProgress$1(url),
    limit
  })
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
  return download$1(url, file, {
    onProgress: !!progress && doProgress(file),
    limit
  })
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

function deserialize (obj) {
  if (Array.isArray(obj)) return Object.freeze(obj.map(deserialize))
  if (obj === null || typeof obj !== 'object') return obj
  if ('$$date$$' in obj) return Object.freeze(new Date(obj.$$date$$))
  if ('$$undefined$$' in obj) return undefined
  return Object.freeze(
    Object.entries(obj).reduce(
      (o, [k, v]) => ({ ...o, [k]: deserialize(v) }),
      {}
    )
  )
}

function serialize (obj) {
  if (Array.isArray(obj)) return obj.map(serialize)
  if (obj === undefined) return { $$undefined$$: true }
  if (obj instanceof Date) return { $$date$$: obj.getTime() }
  if (obj === null || typeof obj !== 'object') return obj
  return Object.entries(obj).reduce(
    (o, [k, v]) => ({ ...o, [k]: serialize(v) }),
    {}
  )
}

const jsonrpc = '2.0';

const knownErrors = {};

class RpcClient {
  constructor (options) {
    this.options = options;
  }

  async call (method, ...params) {
    const body = JSON.stringify({
      jsonrpc,
      method,
      params: serialize(params)
    });

    const options = {
      ...this.options,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json;charset=utf-8',
        'Content-Length': Buffer.byteLength(body),
        Connection: 'keep-alive'
      }
    };
    const res = await makeRequest(options, body);
    const data = await readResponse(res);

    if (data.error) {
      const errDetails = deserialize(data.error);
      const Factory = RpcClient.error(errDetails.name);
      throw new Factory(errDetails)
    }

    return deserialize(data.result)
  }

  static error (name) {
    let constructor = knownErrors[name];
    if (constructor) return constructor
    constructor = makeErrorClass(name);
    knownErrors[name] = constructor;
    return constructor
  }
}

function makeRequest (options, body) {
  return new Promise((resolve, reject) => {
    const req = request(options, resolve);
    req.once('error', reject);
    req.write(body);
    req.end();
  })
}

async function readResponse (res) {
  res.setEncoding('utf8');
  let data = '';
  for await (const chunk of res) {
    data += chunk;
  }
  return JSON.parse(data)
}

function makeErrorClass (name) {
  function fn (data) {
    const { name, ...rest } = data;
    Error.call(this);
    Error.captureStackTrace(this, this.constructor);
    Object.assign(this, rest);
  }

  // reset the name of the constructor
  Object.defineProperties(fn, {
    name: { value: name, configurable: true }
  });

  // make it inherit from error
  fn.prototype = Object.create(Error.prototype, {
    name: { value: name, configurable: true },
    constructor: { value: fn, configurable: true }
  });

  return fn
}

const jsdbMethods = new Set([
  'ensureIndex',
  'deleteIndex',
  'insert',
  'update',
  'upsert',
  'delete',
  'find',
  'findOne',
  'getAll',
  'compact',
  'reload'
]);

const jsdbErrors = new Set(['KeyViolation', 'NotExists', 'NoIndex']);

let client;

const staticMethods = ['status', 'housekeep', 'clear', 'shutdown'];

class Database {
  constructor (opts) {
    /* c8 ignore next 2 */
    if (typeof opts === 'string') opts = { filename: opts };
    const { port = 39720, ...options } = opts;
    this.options = options;
    if (!client) {
      client = new RpcClient({ port });
      for (const method of staticMethods) {
        Database[method] = client.call.bind(client, method);
      }
    }
    const { filename } = this.options;
    for (const method of jsdbMethods.values()) {
      this[method] = client.call.bind(client, 'dispatch', filename, method);
    }
  }

  async check () {
    try {
      await client.call('status');
      /* c8 ignore next 6 */
    } catch (err) {
      if (err.code === 'ECONNREFUSED') {
        throw new NoServer(err)
      } else {
        throw err
      }
    }
  }

  static _reset () {
    client = undefined;
  }
}

class NoServer extends Error {
  constructor (err) {
    super('Could not find jsdbd');
    Object.assign(this, err, { client });
  }
}

Database.NoServer = NoServer;

jsdbErrors.forEach(name => {
  Database[name] = RpcClient.error(name);
});

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

class Local extends EventEmitter {
  constructor (data) {
    super();
    Object.assign(this, data);
  }

  static async * scan (root, filter) {
    root = resolve(root);
    for await (const { path: fullpath, stats } of filescan(root)) {
      if (!stats.isFile()) continue
      const path = relative(root, fullpath);
      if (!filter(path)) continue
      yield new Local({ path, fullpath, root, stats });
    }
  }

  async getHash () {
    if (this.hash) return this.hash
    const db = await getDB$1();
    this.fullpath = await realpath(this.fullpath);
    if (!this.stats) this.stats = await stat$2(this.fullpath);
    const rec = await db.findOne('path', this.fullpath);
    if (rec) {
      if (this.stats.mtimeMs === rec.mtime && this.stats.size === rec.size) {
        this.hash = rec.hash;
        return this.hash
      }
    }

    this.emit('hashing');
    this.hash = await hashFile(this.fullpath);

    await db.upsert({
      ...(rec || {}),
      path: this.fullpath,
      mtime: this.stats.mtimeMs,
      size: this.stats.size,
      hash: this.hash
    });

    return this.hash
  }
}

const getDB$1 = once(async () => {
  const db = new Database('file_md5_cache.db');
  await db.check();
  await db.ensureIndex({ fieldName: 'path', unique: true });
  return db
});

async function hashFile (file) {
  const rs = createReadStream(file);
  const hasher = crypto.createHash('md5');
  for await (const chunk of rs) {
    hasher.update(chunk);
  }
  return hasher.digest('hex')
}

class Remote extends EventEmitter {
  constructor (data) {
    super();
    Object.assign(this, data);
    if (this.etag && !this.etag.includes('-')) {
      this.hash = this.etag;
    }
  }

  static async * scan (root, filter) {
    const { Bucket, Key: Prefix } = parseAddress(root);
    for await (const data of scan(root + '/')) {
      const path = relative(Prefix, data.Key);
      if (data.Key.endsWith('/') || !filter(path)) continue
      yield new Remote({
        path,
        root,
        url: `${Bucket}/${data.Key}`,
        etag: data.ETag.replace(/"/g, '')
      });
    }
  }

  async getHash () {
    if (this.hash) return this.hash
    const db = await getDB();
    const rec = await db.findOne('url', this.url);
    if (rec) {
      if (this.etag === rec.etag) {
        this.hash = rec.hash;
        return this.hash
      }
    }

    this.emit('hashing');
    const stats = await stat$1(`s3://${this.url}`);

    this.hash = stats.md5 || 'UNKNOWN';
    await db.upsert({
      ...(rec || {}),
      url: this.url,
      etag: this.etag,
      hash: this.hash
    });

    return this.hash
  }
}

const getDB = once(async () => {
  const db = new Database('s3file_md5_cache.db');
  await db.check();
  await db.ensureIndex({ fieldName: 'url', unique: true });
  return db
});

// generic matcher
//
// takes a range of streams (as async generators) and matches on keys,
// yielding a tuple of entries
//
// Key is specified as a picking function, or a string (to use that
// property)
//
// If a key is not found in all streams, then the missing entry in the
// tuple will be falsy.
//
// It works best if everything is (mostly) sorted, but can cope with things
// out of order.

async function * match (selectKey, ...sources) {
  selectKey = makeSelector(selectKey);

  // we will store out-of-order things in maps just in case they arrive
  // later. Once we have created a full entry we can yield it out.
  //
  // Once we get to the end, we know we have a collection of partial
  // entries.
  const found = sources.map(() => new Map());

  // read the first item of each source to prefill our head vector
  let heads = await readAll(sources);

  while (true) {
    // If all the heads are off the end, then we have run out of new
    // data, and we exit the loop, prior to cleaning up partials
    //
    if (heads.every(v => !v)) break

    // if all the keys are the same, then we are delightfully in sync
    // We hope this is the most common scenario. We can just yield out
    // the current head tuple and move on
    if (allHaveSameKey(heads)) {
      yield heads;
      heads = await readAll(sources);
      continue
    }

    // Alas we are not in sync. So let's find the earliest key, and
    // pick all the sources on that key
    const currKey = findEarliestKey(heads);
    const matches = heads.map(v => v && selectKey(v) === currKey);

    // if we have found all the missing ones already, then we are
    // okay again. We can extract the missing saved ones, yield the
    // full tuple and move on
    if (matches.every((matched, ix) => matched || found[ix].has(currKey))) {
      const current = heads.map((v, ix) => {
        if (!matches[ix]) {
          v = found[ix].get(currKey);
          found[ix].delete(currKey);
        }
        return v
      });
      yield current;
      heads = await readSome(sources, heads, matches);
      continue
    }

    // Sadly, we do not have enough to complete a full entry, so we
    // store away the ones we have, and advance and go around again
    heads.forEach((v, ix) => {
      if (matches[ix]) found[ix].set(currKey, v);
    });
    heads = await readSome(sources, heads, matches);
  }

  // we have finished all sources, and what we have left in the found
  // maps is a collection of partial entries. So we just go through these
  // in any old order, assembling partials as best we can.

  // first lets get a unique list of all the keys
  const keys = found.reduce(
    (keys, map) => new Set([...keys, ...map.keys()]),
    new Set()
  );

  // and for each key, we assemble the partial entry and yield
  for (const key of keys) {
    const current = heads.map((v, ix) =>
      found[ix].has(key) ? found[ix].get(key) : undefined
    );
    yield current;
  }

  // and we're done.

  function allHaveSameKey (vals) {
    const keys = vals.map(v => (v ? selectKey(v) : null));
    return keys.every(k => k === keys[0])
  }

  function findEarliestKey (vals) {
    return vals.reduce((earliest, v) => {
      if (!v) return earliest
      const k = selectKey(v);
      return !earliest || k < earliest ? k : earliest
    }, null)
  }

  function readSome (gens, curr, matches) {
    return Promise.all(
      gens.map((gen, ix) => (matches[ix] ? readItem(gen) : curr[ix]))
    )
  }

  function readAll (gens) {
    return Promise.all(gens.map(readItem))
  }

  function readItem (gen) {
    return gen.next().then(v => (v.done ? undefined : v.value))
  }

  function makeSelector (sel) {
    return typeof sel === 'function' ? sel : x => x[sel]
  }
}

async function sync (
  lRoot,
  rRoot,
  { dryRun, download: downsync, delete: deleteExtra, ...options }
) {
  lRoot = lRoot.replace(/\/$/, '');
  rRoot = rRoot.replace(/\/$/, '');

  report('sync.start');

  const filter = getFilter(options);
  const lFiles = Local.scan(lRoot, filter);
  const rFiles = Remote.scan(rRoot, filter);

  let fileCount = 0;
  for await (const [local, remote] of match('path', lFiles, rFiles)) {
    fileCount++;
    const path = local ? local.path : remote.path;
    report('sync.file.start', path);
    if (local) {
      local.on('hashing', () => report('sync.file.hashing', path));
      await local.getHash();
    }
    if (remote) {
      remote.on('hashing', () => report('sync.file.hashing', path));
      await remote.getHash();
    }
    if (local && remote) {
      // if they are the same, we can skip this file
      if (local.hash === remote.hash) continue
      if (downsync) {
        await downloadFile(remote);
      } else {
        await uploadFile(local);
      }
    } else if (local) {
      if (downsync) {
        if (deleteExtra) {
          await deleteLocal(local);
        }
      } else {
        await uploadFile(local);
      }
    } else {
      // only remote exists. If uploading, warn about extraneous files, else
      // download it
      if (downsync) {
        await downloadFile(remote);
      } else {
        if (deleteExtra) {
          await deleteRemote(remote);
        }
      }
    }
  }
  report('sync.done', { count: fileCount });

  async function uploadFile ({ path, fullpath }) {
    if (dryRun) {
      report('sync.file.dryrun', { path, action: 'upload' });
      return
    }

    return retry(
      () =>
        upload(fullpath, `${rRoot}/${path}`, {
          ...options,
          progress: true
        }),
      {
        retries: 5,
        delay: 5000,
        onRetry: data => report('retry', data)
      }
    )
  }

  async function downloadFile ({ path, url }) {
    if (dryRun) {
      report('sync.file.dryrun', { path, action: 'download' });
      return
    }

    return retry(
      () =>
        download(url, join(lRoot, path), {
          ...options,
          progress: true
        }),
      {
        retries: 5,
        delay: 5000,
        onRetry: data => report('retry', data)
      }
    )
  }

  async function deleteLocal ({ path }) {
    if (dryRun) {
      report('sync.file.dryrun', { path, action: 'delete' });
      return
    }

    return unlink(join(lRoot, path))
  }

  async function deleteRemote ({ path }) {
    if (dryRun) {
      report('sync.file.dryrun', { path, action: 'delete' });
      return
    }
    const url = `${rRoot}/${path}`;
    report('delete.file.start', url);
    await deleteObject(url);
    report('delete.file.done', url);
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

async function rm (url) {
  report('delete.file.start', url);
  await deleteObject(url);
  report('delete.file.done', url);
}

const prog = sade('s3cli');
const version = '1.6.0';

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
