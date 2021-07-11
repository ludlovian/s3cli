import sade from 'sade'

import ls from './ls.mjs'
import cp from './cp.mjs'
import sync from './sync.mjs'
import rm from './rm.mjs'

const prog = sade('s3cli')
const version = '__VERSION__'

prog.version(version)

prog
  .command('ls <url>', 'list files')
  .option('-r --rescan', 'rescan before listing')
  .option('-l --long', 'show long listing')
  .option('-H --human', 'show amount in human sizes')
  .option('-t --total', 'show grand total')
  .action(ls)

prog
  .command('cp <from> <to>', 'copy a file to/from S3')
  .option('-p, --progress', 'show progress')
  .option('-l, --limit', 'limit rate')
  .option('-q, --quiet', 'no output')
  .action(cp)

prog
  .command('sync <from> <to>', 'sync a directory to/from S3')
  .option('-l, --limit', 'limit rate')
  .option('-n, --dry-run', 'show what would be done')
  .option('-d, --delete', 'delete extra files on the destination')
  .action(sync)

prog
  .command('rm <url>')
  .describe('delete a file')
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
