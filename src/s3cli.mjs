import sade from 'sade'

import ls from './ls.mjs'
import upload from './upload.mjs'
import download from './download.mjs'
import sync from './sync.mjs'
import stat from './stat.mjs'
import rm from './rm.mjs'

const prog = sade('s3cli')
const version = '__VERSION__'

prog.version(version)

prog
  .command('ls <s3url>', 'list the objects in a bucket')
  .option('-l, --long', 'show more detail')
  .option('-t, --total', 'include a total in long listing')
  .option('-H, --human', 'show human sizes in long listing')
  .option('-d, --directory', 'list directories without recursing')
  .action(ls)

prog
  .command('upload <file> <s3url>', 'upload a file to S3')
  .option('-p, --progress', 'show progress')
  .option('-l, --limit', 'limit rate')
  .action(upload)

prog
  .command('download <s3url> <file>', 'download a file from S3')
  .option('-p, --progress', 'show progress')
  .option('-l, --limit', 'limit rate')
  .action(download)

prog
  .command('sync <dir> <s3url>', 'sync a directory with S3')
  .option('-p, --progress', 'show progress')
  .option('-l, --limit', 'limit rate')
  .option('-f, --filter', 'regex to limit the files synced')
  .option('-n, --dry-run', 'show what would be done')
  .option('-d, --delete', 'delete extra files on the destination')
  .option('-D, --download', 'sync from S3 down to local')
  .action(sync)

prog
  .command('stat <s3url>')
  .describe('show details about a file')
  .action(stat)

prog
  .command('rm <s3url>')
  .describe('delete a remote file')
  .action(rm)

const parsed = prog.parse(process.argv, {
  lazy: true,
  alias: { n: ['dryRun', 'dry-run'] }
})

if (parsed) {
  const { args, handler } = parsed
  handler(...args).catch(err => {
    console.error(err)
    process.exit(1)
  })
}
