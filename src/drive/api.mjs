import once from 'pixutil/once'

const getDriveAPI = once(async function getDriveAPI () {
  const scopes = ['https://www.googleapis.com/auth/drive']
  const { default: driveApi } = await import('@googleapis/drive')
  process.env.GOOGLE_APPLICATION_CREDENTIALS = 'credentials.json'
  const auth = new driveApi.auth.GoogleAuth({ scopes })
  const authClient = await auth.getClient()
  return driveApi.drive({ version: 'v3', auth: authClient })
})

export default getDriveAPI
