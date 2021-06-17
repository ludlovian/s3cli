import resolve from '@rollup/plugin-node-resolve'
import replace from '@rollup/plugin-replace'

export default {
  input: 'src/s3cli.mjs',
  external: [ 
    'fs/promises',
    'stream/promises',
    'aws-sdk',
    'better-sqlite3',
    'mime',
    'sade'
  ],
  plugins: [
    resolve({
      preferBuiltins: true
    }),
    replace({
      preventAssignment: true,
      values: {
        __VERSION__: process.env.npm_package_version
      }
    })
  ],
  output: [
    {
      file: 'dist/s3cli.mjs',
      format: 'esm',
      sourcemap: false,
      banner: '#!/usr/bin/env node'
    }
  ]
}
