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
