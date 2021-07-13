import driveAPI from './api.mjs'
import { sql } from '../db/index.mjs'
import { listFiles, insertFile, removeFile } from './sql.mjs'

export default async function * scan (root) {
  const File = root.constructor
  const drive = await driveAPI()

  let n = 0
  const old = new Set(listFiles.all(root).map(r => r.path))
  const insertFiles = sql.transaction(files => {
    for (const file of files) {
      insertFile(file)
      old.delete(file.path)
    }
  })
  const deleteOld = sql.transaction(paths => {
    for (const path of paths) {
      removeFile({ path })
    }
  })

  const query = {
    fields:
      'nextPageToken,files(id,name,mimeType,modifiedTime,size,md5Checksum,parents)'
  }

  const files = new Set()
  let pResponse = drive.files.list(query)
  while (pResponse) {
    const response = await pResponse
    const { status, data } = response
    if (status !== 200) {
      const err = new Error('Bad response from Drive')
      err.response = response
      throw err
    }

    data.files.forEach(row => files.add(new Item(row)))
    // which are ready with their path worked out
    const ready = [...files].filter(f => {
      if (f.findPath() == null) return false
      files.delete(f)
      return true
    })
    // which are actually files under the root
    const found = ready
      .filter(f => !f.isFolder && f.path.startsWith(root.path))
      .map(f => File.like(root, f))

    insertFiles(found)
    n += found.length
    if (found.length) yield n

    if (!data.nextPageToken) break
    query.pageToken = data.nextPageToken
    pResponse = drive.files.list(query)
  }

  deleteOld([...old])
}

const folders = {}
class Item {
  constructor (entry) {
    this.googleId = entry.id
    this.name = entry.name
    this.contentType = entry.mimeType
    if (entry.parents) this.parent = entry.parents[0]
    if (!this.isFolder) {
      this.mtime = new Date(entry.modifiedTime)
      this.size = Number(entry.size)
      this.md5Hash = entry.md5Checksum
    } else {
      folders[this.googleId] = this
    }
  }

  get isFolder () {
    return this.contentType.endsWith('folder')
  }

  findPath () {
    if (!this.path) this.path = calcPath(this)
    return this.path
  }
}

function calcPath (f) {
  if (!f.parent) return '/' + f.name
  const parent = folders[f.parent]
  if (!parent) return undefined
  const pp = calcPath(parent)
  if (pp == null) return undefined
  return pp + '/' + f.name
}
