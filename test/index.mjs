import { test } from 'uvu'
import * as assert from 'uvu/assert'

import { execFileSync } from 'child_process'
import { writeFileSync } from 'fs'
import { resolve } from 'path'

import sync from '../src/sync.mjs'
import cp from '../src/cp.mjs'
import rm from '../src/rm.mjs'
import s3stat from '../src/s3/stat.mjs'
import { open as dbopen } from '../src/db/index.mjs'

const LOCAL_ROOT = resolve('test/assets/') + '/'
const S3_ROOT = 's3://test-readersludlow/s3cli/'

test.before(() => {
  process.env.DB = 'test/test.db'
  dbopen()
  execFileSync('mkdir', ['-p', LOCAL_ROOT])
})

test.after(() => {
  execFileSync('rm', ['-rf', LOCAL_ROOT])
  // execFileSync('rm', ['test/test.db'])
})

test('copy up', async () => {
  const path = 'foo.txt'
  writeFileSync(LOCAL_ROOT + path, 'foobar')

  const lUrl = 'file://' + LOCAL_ROOT + path
  const rUrl = S3_ROOT + path

  // Copy Up
  //
  console.log('\n---- COPY UP ----')
  await cp(lUrl, rUrl)
  let stats = await s3stat(rUrl)
  assert.is(stats.size, 6, 'file copied up okay')
})

test('copy down', async () => {
  console.log('\n---- COPY DOWN ----')

  const path = 'foo.txt'
  const lUrl = 'file://' + LOCAL_ROOT + path
  const rUrl = S3_ROOT + path

  await cp(rUrl, lUrl, { progress: true })

  assert.ok(true)
})

test('sync up unchanged', async () => {
  console.log('\n---- SYNC UP: UNCHANGED ----')
  // Sync up
  //
  await sync(LOCAL_ROOT, S3_ROOT, { limit: 5, delete: true })
  assert.ok(true)
})

test('sync down unchanged', async () => {
  console.log('\n---- SYNC DOWN: UNCHANGED ----')
  // Sync up
  //
  await sync(S3_ROOT, LOCAL_ROOT, { limit: 5, delete: true })
  assert.ok(true)
})

test('sync up rename', async () => {
  console.log('\n---- SYNC UP: RENAME ----')
  // Sync up
  //
  execFileSync('mv', [LOCAL_ROOT + 'foo.txt', LOCAL_ROOT + 'foo2.txt'])

  await sync(LOCAL_ROOT, S3_ROOT, { limit: 5 })
  assert.ok(true)
})

test('sync down rename', async () => {
  console.log('\n---- SYNC DOWN: RENAME ----')
  // Sync up
  //
  execFileSync('mv', [LOCAL_ROOT + 'foo2.txt', LOCAL_ROOT + 'foo.txt'])

  await sync(S3_ROOT, LOCAL_ROOT, { limit: 5 })
  assert.ok(true)
})

test('sync up new files', async () => {
  console.log('\n---- SYNC UP: NEW ----')
  writeFileSync(LOCAL_ROOT + 'foo.txt', 'foobar file 2')
  // Sync up
  //
  await sync(LOCAL_ROOT, S3_ROOT, { limit: 5, delete: true })
  assert.ok(true)
})

test('sync down new files', async () => {
  console.log('\n---- SYNC DOWN: NEW ----')
  // Sync up
  //
  await sync(S3_ROOT, LOCAL_ROOT, { limit: 5, delete: true })
  assert.ok(true)
})

test.skip('sync up', async () => {
  // Sync changes
  writeFileSync(LOCAL_ROOT + path, 'foobar2')
  await sync(LOCAL_ROOT, S3_ROOT)
  stats = await vfs.stat(rUrl)
  assert.is(stats.size, 7, 'changed file synced up okay')

  // Sync down (no changes)
  await sync(S3_ROOT, LOCAL_ROOT)

  // Sync down (dry run)
  console.log('\nShould now dry run...\n')
  await rm(lUrl, { dryRun: true })
  await rm(lUrl)

  await sync(S3_ROOT, LOCAL_ROOT, { dryRun: true })

  // Sync down
  await sync(S3_ROOT, LOCAL_ROOT)

  // Sync deletion up
  await rm(lUrl)
  await sync(LOCAL_ROOT, S3_ROOT)
  await sync(LOCAL_ROOT, S3_ROOT, { delete: true })
})

test.run()
