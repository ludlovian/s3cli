import AWS from 'aws-sdk'

import log from 'logjs'
import once from 'pixutil/once'

import { comma, fmtTime, fmtSize } from '../util.mjs'

const getS3 = once(() => {
  const REGION = 'eu-west-1'
  return new AWS.S3({ region: REGION })
})
export { getS3 }

export function parse (url) {
  const m = /^s3:\/\/([^/]+)(?:\/(.*))?$/.exec(url)
  if (!m) throw new TypeError(`Bad S3 URL: ${url}`)
  return { bucket: m[1], path: m[2] || '' }
}

export function onProgress ({ speedo }) {
  const { done, current, percent, total, taken, eta, rate } = speedo
  if (!done) {
    const s = [
      comma(current).padStart(1 + comma(total).length),
      `${percent.toString().padStart(3)}%`,
      `time ${fmtTime(taken)}`,
      `eta ${fmtTime(eta)}`,
      `rate ${fmtSize(rate)}B/s`
    ].join(' ')
    log.status(s)
  } else {
    const s = [
      ` ${comma(total)} bytes copied`,
      `in ${fmtTime(taken)}`,
      `at ${fmtSize(rate)}B/s`
    ].join(' ')
    log(log.green(s))
  }
}
