export function getDirection (src, dst) {
  const validDirections = new Set(['local_s3', 's3_local'])
  const dir = src.type + '_' + dst.type
  if (!validDirections.has(dir)) {
    throw new Error(`Cannot do ${src.type} -> ${dst.type}`)
  }
  return dir
}

export function isUploading (src, dst) {
  if (src.isLocal && dst.isS3) return true
  if (src.isS3 && dst.isLocal) return false
  throw new Error('Must either upload or download')
}

export function comma (n) {
  if (typeof n !== 'number') return n
  return n.toLocaleString()
}

export function fmtTime (t) {
  t = Math.round(t / 1000)
  if (t < 60) return t + 's'
  t = Math.round(t / 60)
  return t + 'm'
}

export function fmtSize (n) {
  const suffixes = [
    ['G', 1024 * 1024 * 1024],
    ['M', 1024 * 1024],
    ['K', 1024],
    ['', 1]
  ]

  for (const [suffix, factor] of suffixes) {
    if (n >= factor) return (n / factor).toFixed(1) + suffix
  }
  return '0'
}

export function plural (count, noun) {
  return comma(count) + ' ' + noun + (count === 1 ? '' : 's')
}
