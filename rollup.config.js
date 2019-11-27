import { terser } from 'rollup-plugin-terser'
import cleanup from 'rollup-plugin-cleanup'
import json from 'rollup-plugin-json'
import resolve from 'rollup-plugin-node-resolve'
import commonjs from 'rollup-plugin-commonjs'

export default {
  input: 'src/s3cli.js',
  external: [
    'aws-sdk',
    'events',
    'path',
    'crypto',
    'fs',
    'stream',
    'util',
    'net',
    'http'
  ],
  plugins: [
    resolve(),
    commonjs(),
    json(),
    cleanup(),
    process.env.NODE_ENV === 'production' && terser()
  ],
  output: [
    {
      file: 'dist/s3cli',
      format: 'cjs',
      sourcemap: false,
      banner: '#!/usr/bin/env node'
    }
  ]
}
