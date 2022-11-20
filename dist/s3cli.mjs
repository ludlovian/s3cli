#!/usr/bin/env node
import sade from 'sade';
import { format } from 'util';
import { createReadStream, realpathSync, createWriteStream } from 'fs';
import mime from 'mime';
import { readdir, lstat, mkdir, utimes, copyFile, unlink, rmdir } from 'fs/promises';
import { join, dirname } from 'path';
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

const DB_VERSION = 5;

const db = {};

let opened = false;

function open () {
  if (opened) return
  opened = true;

  const dbFile = process.env.DB || join(homedir(), '.databases', 'files.sqlite');
  const _db = new SQLite(dbFile);
  _db.pragma('foreign_keys=ON');
  if (getVersion(_db) !== DB_VERSION) {
    throw new Error('Wrong version of db: ' + dbFile)
  }

  Object.assign(db, buildStoredProcs(_db));
  Object.assign(db, buildQueries(_db));
}

db.open = open;

function getVersion (db) {
  const sql = 'select version from dbversion';
  let p = db.prepare(sql);
  p = p.pluck();
  return p.get()
}

function buildStoredProcs (db) {
  const insertLocalFile = makeStoredProc(db, 'sp_insertLocalFile');
  const deleteLocalFile = makeStoredProc(db, 'sp_deleteLocalFile');
  const insertS3File = makeStoredProc(db, 'sp_insertS3File');
  const deleteS3File = makeStoredProc(db, 'sp_deleteS3File');
  const insertGdriveFile = makeStoredProc(db, 'sp_insertGdriveFile');
  const deleteGdriveFile = makeStoredProc(db, 'sp_deleteGdriveFile');
  return {
    insertLocalFiles: db.transaction(files => files.forEach(insertLocalFile)),
    insertS3Files: db.transaction(files => files.forEach(insertS3File)),
    insertGdriveFiles: db.transaction(files => files.forEach(insertGdriveFile)),
    deleteLocalFiles: db.transaction(files => files.forEach(deleteLocalFile)),
    deleteS3Files: db.transaction(files => files.forEach(deleteS3File)),
    deleteGdriveFiles: db.transaction(files => files.forEach(deleteGdriveFile))
  }
}

function buildQueries (db) {
  const ret = {};
  for (const [name, entry] of Object.entries(QUERIES)) {
    const [sql, params] = entry;
    const stmt = db.prepare(sql);
    if (params.length) {
      ret[name] = x => stmt.all(chk(x, ...params));
    } else {
      ret[name] = () => stmt.all();
    }
  }

  // adjust exceptions
  {
    const fn = ret.locateLocalFile;
    ret.locateLocalFile = x => fn(x)[0];
  }

  {
    const fn = ret.getDuplicates;
    ret.getDuplicates = () => fn().map(x => x.contentId);
  }
  return ret
}

function makeStoredProc (db, name, noParams = false) {
  if (noParams) {
    const sql = `insert into ${name} values(null)`;
    const stmt = db.prepare(sql);
    return () => stmt.run()
  } else {
    const params = getProcParams(db, name);
    const sql =
      `insert into ${name}(${params.join(',')}) ` +
      `values(${params.map(n => ':' + n).join(',')})`;
    const stmt = db.prepare(sql);
    return data => stmt.run(chk(data, ...params))
  }
}

function getProcParams (db, name) {
  const sql = `select * from ${name}`;
  return db
    .prepare(sql)
    .columns()
    .map(col => col.name)
}

function chk (data, ...params) {
  const ret = {};
  const supplied = Object.getOwnPropertyNames(data);
  for (const param of params) {
    if (!supplied.includes(param)) {
      console.error('Was expecting to see "%s" in "%o"', param, data);
      throw new Error('Bad parameter given')
    }
    let v = data[param];
    if (v instanceof Date) v = v.toISOString();
    ret[param] = v;
  }
  return ret
}

const QUERIES = {
  getLocalFiles: [
    "select * from qry_localFiles where path glob :path || '*'",
    ['path']
  ],
  getS3Files: [
    'select * from qry_s3Files ' +
      "where bucket = :bucket and path glob :path || '*'",
    ['bucket', 'path']
  ],
  getGdriveFiles: [
    "select * from qry_gdriveFiles where path glob :path || '*'",
    ['path']
  ],
  locateLocalFile: [
    'select * from qry_localFiles ' +
      'where path = :path and size = :size and mtime = datetime(:mtime)',
    ['path', 'size', 'mtime']
  ],
  getLocalNotS3: [
    'select * from qry_localFiles ' +
      "where path glob :localPath || '*' " +
      'and contentId not in (' +
      'select contentId from qry_s3Files ' +
      "where bucket = :s3Bucket and path glob :s3Path || '*')",
    ['localPath', 's3Bucket', 's3Path']
  ],
  getS3NotLocal: [
    'select * from qry_s3Files ' +
      "where bucket = :s3Bucket and path glob :s3Path || '*' " +
      'and contentId not in (' +
      'select contentId from qry_localFiles ' +
      "where path glob :localPath || '*')",
    ['localPath', 's3Bucket', 's3Path']
  ],
  getLocalNotGdrive: [
    'select * from qry_localFiles ' +
      "where path glob :localPath || '*' " +
      'and contentId not in (' +
      'select contentId from qry_gdriveFiles ' +
      "where path glob :gdrivePath || '*')",
    ['localPath', 'gdrivePath']
  ],
  getGdriveNotLocal: [
    'select * from qry_gdriveFiles ' +
      "where path glob :gdrivePath || '*' " +
      'and contentId not in (' +
      'select contentId from qry_localFiles ' +
      "where path glob :localPath || '*')",
    ['localPath', 'gdrivePath']
  ],
  getLocalS3Diffs: [
    'select * from qry_localAndS3 ' +
      "where localPath glob :localPath || '*' " +
      'and s3Bucket = :s3Bucket ' +
      "and s3Path glob :s3Path || '*' " +
      'and substr(localPath, 1+length(:localPath)) != ' +
      'substr(s3Path, 1 + length(:s3Path))',
    ['localPath', 's3Bucket', 's3Path']
  ],
  getLocalGdriveDiffs: [
    'select * from qry_localAndGdrive ' +
      "where localPath glob :localPath || '*' " +
      "and gdrivePath glob :gdrivePath || '*' " +
      'and substr(localPath, 1+length(:localPath)) != ' +
      'substr(gdrivePath, 1 + length(:gdrivePath))',
    ['localPath', 'gdrivePath']
  ],
  getLocalContent: [
    'select * from qry_localFiles ' +
      'where contentId = :contentId ' +
      "and path glob :localPath || '*'",
    ['contentId', 'localPath']
  ],
  getS3Content: [
    'select * from qry_s3Files ' +
      'where contentId = :contentId ' +
      'and bucket = :s3Bucket ' +
      "and path glob :s3Path || '*'",
    ['contentId', 's3Bucket', 's3Path']
  ],
  getGdriveContent: [
    'select * from qry_gdriveFiles ' +
      'where contentId = :contentId ' +
      "and path glob :gdrivePath || '*'",
    ['contentId', 'gdrivePath']
  ],
  getDuplicates: ['select * from qry_duplicates', []]
};

async function * scan$2 (root) {
  const File = root.constructor;

  let n = 0;
  const old = new Set(db.getLocalFiles(root).map(f => f.path));

  for await (const files of scanDir(root.path, File)) {
    n += files.length;
    db.insertLocalFiles(files);
    files.forEach(f => old.delete(f.path));
    yield n;
  }
  db.deleteLocalFiles([...old].map(path => ({ path })));
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

async function hashFile (
  filename,
  { algo = 'md5', enc = 'hex' } = {}
) {
  const hasher = createHash(algo);
  for await (const chunk of createReadStream(filename)) {
    hasher.update(chunk);
  }
  return hasher.digest(enc)
}

async function stat$2 (file) {
  const stats = await lstat(file.path);
  file.size = stats.size;
  file.mtime = stats.mtime;
  const f = db.locateLocalFile(file);
  if (f) file.md5Hash = f.md5Hash;
  if (!file.md5Hash) {
    log.status('%s ... hashing', file.path);
    file.md5Hash = await hashFile(file.path);
    db.insertLocalFiles([file]);
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

function getDirection (src, dst) {
  const validDirections = new Set(['local_s3', 's3_local', 'gdrive_local']);
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

async function * scan$1 (root) {
  const File = root.constructor;
  const { bucket } = root;
  let n = 0;
  const s3 = getS3();

  const old = new Set(db.getS3Files(root).map(r => r.path));

  const request = { Bucket: bucket, Prefix: root.path };
  while (true) {
    const result = await s3.listObjectsV2(request).promise();
    const files = [];
    for (const item of result.Contents) {
      const file = new File({
        type: 's3',
        bucket,
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
    db.insertS3Files(files);
    files.forEach(f => old.delete(f.path));
    yield n;

    if (!result.IsTruncated) break
    request.ContinuationToken = result.NextContinuationToken;
  }

  db.deleteS3Files([...old].map(path => ({ bucket, path })));
}

const MD_KEY = 's3cmd-attrs';

async function stat$1 (file) {
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

const PROJECT_DIR = '/home/alan/dev/s3cli/';
const CREDENTIALS = PROJECT_DIR + 'credentials.json';

const getDriveAPI = once(async function getDriveAPI () {
  const scopes = ['https://www.googleapis.com/auth/drive'];
  const { default: driveApi } = await import('@googleapis/drive');
  process.env.GOOGLE_APPLICATION_CREDENTIALS = CREDENTIALS;
  const auth = new driveApi.auth.GoogleAuth({ scopes });
  const authClient = await auth.getClient();
  return driveApi.drive({ version: 'v3', auth: authClient })
});

async function * scan (root) {
  const File = root.constructor;
  const drive = await getDriveAPI();

  let n = 0;
  const old = new Set(db.getGdriveFiles(root).map(r => r.path));
  const query = {
    fields:
      'nextPageToken,files(id,name,mimeType,modifiedTime,size,md5Checksum,parents)'
  };

  const files = new Set();
  let pResponse = drive.files.list(query);
  while (pResponse) {
    const response = await pResponse;
    const { status, data } = response;
    if (status !== 200) {
      const err = new Error('Bad response from Drive');
      err.response = response;
      throw err
    }

    data.files.forEach(row => files.add(new Item(row)));
    // which are ready with their path worked out
    const ready = [...files].filter(f => {
      if (f.findPath() == null) return false
      files.delete(f);
      return true
    });
    // which are actually files under the root
    const found = ready
      .filter(f => !f.isFolder && f.path.startsWith(root.path))
      .map(f => File.like(root, f));

    if (found.length) {
      db.insertGdriveFiles(found);
      found.forEach(f => old.delete(f.path));
      n += found.length;
      yield n;
    }

    if (!data.nextPageToken) break
    query.pageToken = data.nextPageToken;
    pResponse = drive.files.list(query);
  }

  db.deleteGdriveFiles([...old].map(path => ({ path })));
}

const folders = {};
class Item {
  constructor (entry) {
    this.googleId = entry.id;
    this.name = entry.name;
    this.contentType = entry.mimeType;
    if (entry.parents) this.parent = entry.parents[0];
    if (!this.isFolder) {
      this.mtime = new Date(entry.modifiedTime);
      this.size = Number(entry.size);
      this.md5Hash = entry.md5Checksum;
    } else {
      folders[this.googleId] = this;
    }
  }

  get isFolder () {
    return this.contentType.endsWith('folder')
  }

  findPath () {
    if (!this.path) this.path = calcPath(this);
    return this.path
  }
}

function calcPath (f) {
  if (!f.parent) return '/' + f.name
  const parent = folders[f.parent];
  if (!parent) return undefined
  const pp = calcPath(parent);
  if (pp == null) return undefined
  return pp + '/' + f.name
}

async function stat (file) {
  const row = db.getGDriveFiles(file)[0];
  if (!row) {
    throw new Error('Not found: ' + file.url)
  }
  file.googleId = row.googleId;
  file.mtime = new Date(row.mtime + 'Z');
  file.size = row.size;
  file.contentType = row.contentType;
  file.md5Hash = row.md5Hash;
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
    } else if (url.startsWith('gdrive://')) {
      let path = url.slice(9);
      path = maybeAddSlash(path, directory);
      return new File({ type: 'gdrive', path })
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
    } else if (this.type === 'gdrive') {
      this.path = data.path;
      this.googleId = data.googleId;
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

  get isGdrive () {
    return this.type === 'gdrive'
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
    } else if (this.isLocal) {
      return `file://${this.path}`
    } else {
      return `gdrive://${this.path}`
    }
  }

  async stat () {
    if (this.hasStats) return
    const fn = {
      s3: stat$1,
      local: stat$2,
      gdrive: stat
    }[this.type];

    if (fn) await fn(this);
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
      local: scan$2,
      s3: scan$1,
      gdrive: scan
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
  db.open();
  const { long, rescan, human, total } = opts;
  url = File.fromUrl(url, { resolve: true });

  const list = {
    local: db.getLocalFiles,
    s3: db.getS3Files,
    gdrive: db.getGdriveFiles
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
  db.insertS3Files([dest]);
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

async function download$1 (source, dest, opts = {}) {
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

  db.insertLocalFiles([{ ...dest, md5Hash, size, mtime }]);
}

async function download (src, dst, opts = {}) {
  const { size, mtime, md5Hash, googleId } = src;
  const { dryRun, progress, interval = 1000, limit } = opts;

  if (dryRun) {
    log.colour('cyan')('%s downloaded (dryrun)', dst.path);
    return
  }

  if (progress) log(log.cyan(dst.path));

  await mkdir(dirname(dst.path), { recursive: true });

  const drive = await getDriveAPI();
  const hasher = hashStream();
  const speedo$1 = speedo({ total: size });
  const req = { fileId: googleId, alt: 'media' };
  const reqOpts = { responseType: 'stream' };
  const streams = [
    (await drive.files.get(req, reqOpts)).data,
    hasher,
    limit && throttle(limit),
    progress && speedo$1,
    progress && progressStream({ onProgress, interval, speedo: speedo$1 }),
    createWriteStream(dst.path)
  ].filter(Boolean);

  await pipeline(...streams);

  if (mtime) await utimes(dst.path, mtime, mtime);
  db.insertLocalFiles([{ ...dst, size, mtime, md5Hash }]);
}

async function cp (src, dst, opts = {}) {
  db.open();
  src = File.fromUrl(src, { resolve: true });
  dst = File.fromUrl(dst, { resolve: true });
  const dir = getDirection(src, dst);
  const fns = {
    local_s3: upload,
    s3_local: download$1,
    gdrive_local: download
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

  db.insertLocalFiles([{ ...from, path: to.path }]);
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
  db.deleteLocalFiles([file]);
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
  db.insertS3Files([to]);

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
  db.deleteS3Files([file]);
  log(log.cyan(`${file.url} removed`));
}

async function sync (srcRoot, dstRoot, opts = {}) {
  db.open();
  srcRoot = File.fromUrl(srcRoot, { resolve: true, directory: true });
  dstRoot = File.fromUrl(dstRoot, { resolve: true, directory: true });
  const fn = getFunctions(srcRoot, dstRoot);

  await srcRoot.scan();
  await dstRoot.scan();

  const seen = new Set();

  // new files
  for (const row of fn.newFiles()) {
    const src = File.like(srcRoot, row);
    const dst = src.rebase(srcRoot, dstRoot);
    await fn.copy(src, dst, { ...opts, progress: true });
    seen.add(dst.path);
  }

  // simple renames
  for (const { contentId } of fn.differences()) {
    const src = File.like(srcRoot, fn.srcContent(contentId)[0]);
    const dst = src.rebase(srcRoot, dstRoot);
    const old = File.like(dstRoot, fn.dstContent(contentId)[0]);
    if (!old.archived) {
      await fn.destCopy(old, dst, opts);
    } else {
      await fn.copy(src, dst, { ...opts, progress: true });
    }
    seen.add(dst.path);
  }

  // complex renames
  for (const contentId of fn.duplicates()) {
    const srcs = fn.srcContent(contentId).map(row => File.like(srcRoot, row));
    const dsts = fn.dstContent(contentId).map(row => File.like(dstRoot, row));
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
    for (const row of fn.oldFiles()) {
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
    s3Path: (src.isS3 && src.path) || (dst.isS3 && dst.path),
    gdrivePath: (src.isGdrive && src.path) || (dst.isGdrive && dst.path)
  };
  return {
    local_s3: {
      newFiles: () => db.getLocalNotS3(paths),
      oldFiles: () => db.getS3NotLocal(paths),
      differences: () => db.getLocalS3Diffs(paths),
      duplicates: () => db.getDuplicates(),
      srcContent: contentId => db.getLocalContent({ ...paths, contentId }),
      dstContent: contentId => db.getS3Content({ ...paths, contentId }),
      copy: upload,
      destCopy: copy,
      remove: remove
    },
    s3_local: {
      newFiles: () => db.getS3NotLocal(paths),
      oldFiles: () => db.getLocalNotS3(paths),
      differences: () => db.getLocalS3Diffs(paths),
      duplicates: () => db.getDuplicates(),
      srcContent: contentId => db.getS3Content({ ...paths, contentId }),
      dstContent: contentId => db.getLocalContent({ ...paths, contentId }),
      copy: download$1,
      destCopy: copy$1,
      remove: remove$1
    },
    gdrive_local: {
      newFiles: () => db.getGdriveNotLocal(paths),
      oldFiles: () => db.getLocalNotGdrive(paths),
      differences: () => db.getLocalGdriveDiffs(paths),
      duplicates: () => db.getDuplicates(),
      srcContent: contentId => db.getGdriveContent({ ...paths, contentId }),
      dstContent: contentId => db.getLocalContent({ ...paths, contentId }),
      copy: download,
      destCopy: copy$1,
      remove: remove$1
    }
  }[dir]
}

async function rm (file, opts = {}) {
  db.open();
  file = File.fromUrl(file, { resolve: true });
  const fns = {
    local: remove$1,
    s3: remove
  };
  const fn = fns[file.type];

  if (!fn) throw new Error('Cannot rm ' + file.url)

  await file.stat();
  await fn(file, opts);
}

const prog = sade('s3cli');
const version = '2.2.7';

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
