const validProtocols = /^(?:s3|file):\/\//

export function validateUrl (url, { dir } = {}) {
  if (url.startsWith('/')) url = 'file://' + url
  /* c8 ignore next 3 */
  if (!validProtocols.test(url)) {
    throw new Error('Unknown type of URI: ' + url)
  }
  /* c8 ignore next */
  if (dir && !url.endsWith('/')) url += '/'
  return url
}
