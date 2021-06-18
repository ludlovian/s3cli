import sade from 'sade'

import ls from './ls.mjs'
import cp from './cp.mjs'
import sync from './sync.mjs'
import rm from './rm.mjs'

const prog = sade('s3cli')
const version = '__VERSION__'

prog.version(version)

prog
  .command('ls <url>', 'list the files under a dir')
  .option('-l, --long', 'show more detail')
  .option('-t, --total', 'include a total in long listing')
  .option('-H, --human', 'show human sizes in long listing')
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
  .option('-f, --filter', 'apply a regexp filter to the pathnames')
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
