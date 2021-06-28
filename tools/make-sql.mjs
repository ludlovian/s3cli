import { readFileSync, readdirSync } from 'fs'
import { join } from 'path'
import { tidy } from '../src/lib/sql.mjs'

const dir = process.argv[2]

console.log("import SQL from '../lib/sql.mjs'")

for (const name of readdirSync(dir)) {
  const file = join(dir, name)
  const t = JSON.stringify(tidy(readFileSync(file, 'utf8')))
  const v = name.split('.')[0]
  console.log(`export const ${v}=SQL.from(${t});`)
}
