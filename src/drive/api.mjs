import { fileURLToPath } from 'url'
import { resolve } from 'path'
import once from 'pixutil/once'

const getDriveAPI = once(async function getDriveAPI () {
  const scopes = ['https://www.googleapis.com/auth/drive']
  const { default: driveApi } = await import('@googleapis/drive')
  const credentials = resolve(
    fileURLToPath(import.meta.url),
    '../../credentials.json'
  )
  process.env.GOOGLE_APPLICATION_CREDENTIALS = credentials
  const auth = new driveApi.auth.GoogleAuth({ scopes })
  const authClient = await auth.getClient()
  return driveApi.drive({ version: 'v3', auth: authClient })
})

export default getDriveAPI
