import EventEmitter from 'events'
import { format as formatTime } from '@lukeed/ms'
import tinydate from 'tinydate'

import log from 'logjs'

const reporter = new EventEmitter()
const { green, cyan } = log

export default function report (msg, payload) {
  reporter.emit(msg, payload)
}

/* c8 ignore start */
reporter
  .on('list.file', data => {
    let s = ''
    if (data.long) {
      s += data.storage.padEnd(1) + '  '
      const size = data.human ? fmtSize(data.size) : data.size.toString()
      s += size.padStart(10) + '  '
      s += fmtDate(data.mtime).padEnd(18) + '  '
    }
    log(s + data.path)
  })
  .on('list.file.totals', ({ totalSize, totalCount, total, human }) => {
    if (!total) return
    const s = human ? `${fmtSize(totalSize)}B` : `${comma(totalSize)} bytes`
    log(`\n${s} in ${comma(totalCount)} file${totalCount > 1 ? 's' : ''}`)
  })
  .on('cp', opts => opts.quiet || log(opts.url))
  .on('cp.start', url => log(cyan(url)))
  .on('cp.update', data => {
    const { bytes, percent, total, taken, eta, speed } = data
    log.status(
      [
        comma(bytes).padStart(1 + comma(total).length),
        `${percent.toString().padStart(3)}%`,
        `time ${fmtTime(taken)}`,
        `eta ${fmtTime(eta)}`,
        `rate ${fmtSize(speed)}B/s`
      ].join(' ')
    )
  })
  .on('cp.done', ({ bytes, taken, speed }) => {
    log(
      green(
        [
          ` ${comma(bytes)} bytes copied`,
          `in ${fmtTime(taken, true)}`,
          `at ${fmtSize((bytes * 1e3) / taken)}B/s`
        ].join(' ')
      )
    )
  })
  .on('cp.dryrun', ({ url }) => log(`${url} - copied (dry run)`))
  .on('sync.scan.start', () => log.status(`Scanning ... `))
  .on('sync.scan', ({ count }) => log.status(`Scanning ... ${count}`))
  .on('sync.scan.done', () => log.status(''))
  .on('sync.start', () => log.status('Scanning files'))
  .on('sync.hash', url => log.status(`${url} - hashing`))
  .on('sync.done', count =>
    log(`${comma(count)} file${count > 1 ? 's' : ''} processed.`)
  )
  .on('rm.dryrun', url => log(`${url} - deleted (dry run)`))
  .on('rm', url => log(`${url} - deleted`))
  .on('retry', ({ delay, error }) => {
    console.error(
      `\nError occured: ${error.message}\nWaiting ${fmtTime(delay)} to retry...`
    )
  })

function fmtTime (ms) {
  if (ms < 1000) ms = 1000 * Math.round(ms / 1000)
  if (!ms) return '0s'
  return formatTime(ms)
}

function fmtSize (n) {
  const suffixes = [
    ['G', 1024 * 1024 * 1024],
    ['M', 1024 * 1024],
    ['K', 1024],
    ['', 1]
  ]

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
  MMM: d => d.toLocaleString(undefined, { month: 'short' }).slice(0, 3)
})
/* c8 ignore end */
