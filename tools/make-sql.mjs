import { readFileSync, readdirSync } from 'fs'
import { join } from 'path'

const dir = process.argv[2]
console.log('export const sql={};')
for (let name of readdirSync(dir)) {
  let text = readFileSync(join(dir, name), 'utf8')
  text = text
    .split('\n')
    .map(line => line.trim())
    .map(line => line.replaceAll(/  +/g, ' '))
    .filter(Boolean)
    .filter(line => !/^--/.test(line))
    .join(' ')
  text = JSON.stringify(text)
  name = name.split('.')[0]
  if (name === 'ddl') console.log(`export const ddl=${text}`)
  else console.log(`sql.${name}=${text};`)
}
