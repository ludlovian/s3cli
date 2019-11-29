'use strict'

import sade from 'sade'

import { wrap } from './util'
import { version } from '../package.json'

import ls from './ls'
import upload from './upload'
import download from './download'
import sync from './sync'

const prog = sade('s3cli')

prog.version(version)

prog
  .command('ls <s3url>', 'list the objects in a bucket')
  .option('-l, --long', 'show more detail')
  .option('-t, --total', 'include a total in long listing')
  .option('-H, --human', 'show human sizes in long listing')
  .option('-d, --directory', 'list directories without recursing')
  .action(wrap(ls))

prog
  .command('upload <file> <s3url>', 'upload a file to S3')
  .option('-p, --progress', 'show progress')
  .option('-l, --limit', 'limit rate')
  .action(wrap(upload))

prog
  .command('download <s3url> <file>', 'download a file from S3')
  .option('-p, --progress', 'show progress')
  .option('-l, --limit', 'limit rate')
  .action(wrap(download))

prog
  .command('sync <dir> <s3url>', 'sync a directory with S3')
  .option('-p, --progress', 'show progress')
  .option('-l, --limit', 'limit rate')
  .option('-f, --filter', 'regex to limit the files synced')
  .option('-n, --dry-run', 'show what would be done')
  .option('-d, --delete', 'delete extra files on the destination')
  .option('-D, --download', 'sync from S3 down to local')
  .action(wrap(sync))

prog.parse(process.argv, {
  alias: { n: ['dryRun', 'dry-run'] }
})
