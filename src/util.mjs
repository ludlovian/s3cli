const validProtocols = /^(?:s3|file):\/\//

export function validateUrl (url, { dir } = {}) {
  if (url.startsWith('/')) url = 'file://' + url
  if (!validProtocols.test(url)) {
    throw new Error('Unknown type of URI: ' + url)
  }
  if (dir && !url.endsWith('/')) url += '/'
  return url
}
