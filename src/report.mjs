import EventEmitter from 'events'
import { format } from '@lukeed/ms'
import tinydate from 'tinydate'

import log from 'logjs'

const reporter = new EventEmitter()
const { green, cyan } = log

export default function report (msg, payload) {
  reporter.emit(msg, payload)
}

reporter
  .on('list.file', data => {
    if (data.long) {
      let type
      let size = ''
      let time = ''

      if (data.directory) {
        type = 'D'
      } else {
        type = data.storageClass
        size = data.human ? fmtSize(data.size) : data.size.toString()
        if (data.mtime) time = fmtDate(data.mtime)
      }
      log(
        [type.padEnd(1), size.padStart(10), time.padEnd(18), data.key].join(
          '  '
        )
      )
    } else {
      log(data.key)
    }
  })
  .on('list.file.totals', ({ totalSize, totalCount, total, human }) => {
    if (!total) return
    const s = human ? `${fmtSize(totalSize)}B` : `${comma(totalSize)} bytes`
    log(`\n${s} in ${comma(totalCount)} file${totalCount > 1 ? 's' : ''}`)
  })
  .on('file.transfer.start', url => log(cyan(url)))
  .on(
    'file.transfer.update',
    ({ bytes, percent, total, taken, eta, speed }) => {
      log.status(
        [
          comma(bytes).padStart(1 + comma(total).length),
          `${percent.toString().padStart(3)}%`,
          `time ${format(taken)}`,
          `eta ${eta < 1000 ? '0s' : format(eta)}`,
          `rate ${fmtSize(speed)}B/s`
        ].join(' ')
      )
    }
  )
  .on('file.transfer.done', ({ bytes, taken, speed, direction }) => {
    log(
      green(
        [
          ` ${comma(bytes)} bytes`,
          direction,
          `in ${format(taken, true)}`,
          `at ${fmtSize((bytes * 1e3) / taken)}B/s`
        ].join(' ')
      )
    )
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
      `\nError occured: ${error.message}\nWaiting ${format(delay)} to retry...`
    )
  })
  .on('stat.start', url => log(url + '\n'))
  .on('stat.details', ({ key, value, width }) =>
    log(
      [
        green(`${key}:`.padEnd(width + 2)),
        value instanceof Date ? fmtDate(value) : value
      ].join('')
    )
  )

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
  MMM: d => d.toLocaleString(undefined, { month: 'short' })
})
