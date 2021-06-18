import { test } from 'uvu'
import * as assert from 'uvu/assert'

import { execFileSync } from 'child_process'
import { writeFileSync } from 'fs'
import { resolve } from 'path'

import sync from '../src/sync.mjs'
import cp from '../src/cp.mjs'
import rm from '../src/rm.mjs'
import * as vfs from '../src/vfs.mjs'

const LOCAL_ROOT = resolve('test/assets/') + '/'
const S3_ROOT = 's3://test-readersludlow/s3cli/'

test.before(() => {
  execFileSync('mkdir', ['-p', LOCAL_ROOT])
})

test.after(() => {
  execFileSync('rm', ['-rf', LOCAL_ROOT])
})

test('main ops', async () => {
  const path = 'foo.txt'
  writeFileSync(LOCAL_ROOT + path, 'foobar')

  const lUrl = 'file://' + LOCAL_ROOT + path
  const rUrl = S3_ROOT + path

  // Copy Up
  //
  await cp(lUrl, rUrl)
  let stats = await vfs.stat(rUrl)
  assert.is(stats.size, 6, 'file copied up okay')

  // Copy Down
  //
  await rm(lUrl)
  await cp(rUrl, lUrl, { quiet: true })
  stats = await vfs.stat(lUrl)
  assert.is(stats.size, 6, 'file copied down okay')

  // Sync up
  //
  await rm(rUrl)
  await sync(LOCAL_ROOT, S3_ROOT, { limit: 5 })
  stats = await vfs.stat(rUrl)
  assert.is(stats.size, 6, 'file synced up okay')

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
