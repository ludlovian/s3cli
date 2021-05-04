import { join, relative, dirname, basename } from 'path'

export function urljoin (base, file) {
  const url = new URL(base)
  url.pathname = join(url.pathname, file)
  return url.href
}

export function urlrelative (from, to) {
  from = new URL(from)
  to = new URL(to)
  return relative(from.pathname || '/', to.pathname)
}

export function urldirname (url) {
  url = new URL(url)
  url.pathname = dirname(url.pathname)
  return url.href
}

export function urlbasename (url) {
  url = new URL(url)
  return basename(url.pathname)
}
