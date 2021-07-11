import { getDB } from '../db/index.mjs'

const SQL = {
  from: sql => statement({ stmts: tidy(sql).split(';') }),
  transaction: fn => transaction(fn)
}

export default SQL

function statement (data) {
  function exec (...args) {
    args = args.map(cleanArgs)
    const { stmts, pluck, raw, get, all } = data
    const n = stmts.length
    for (let i = 0; i < n - 1; i++) {
      prepare(stmts[i]).run(...args)
    }
    let stmt = prepare(stmts[n - 1])
    if (pluck) stmt = stmt.pluck()
    if (raw) stmt = stmt.raw()
    if (get) return stmt.get(...args)
    if (all) return stmt.all(...args)
    return stmt.run(...args)
  }
  return Object.defineProperties(exec, {
    sql: { value: data.stmts.join(';') },
    pluck: { value: () => statement({ ...data, pluck: true }) },
    raw: { value: () => statement({ ...data, raw: true }) },
    get: { get: () => statement({ ...data, get: true }) },
    all: { get: () => statement({ ...data, all: true }) }
  })
}

const cache = new Map()
function prepare (stmt) {
  let p = cache.get(stmt)
  if (p) return p
  p = getDB().prepare(stmt)
  cache.set(stmt, p)
  return p
}

function cleanArgs (x) {
  if (!x || typeof x !== 'object') return x
  x = { ...x }
  for (const k in x) {
    if (x[k] instanceof Date) x[k] = x[k].toISOString()
  }
  return x
}

function transaction (_fn) {
  let fn
  return (...args) => {
    if (!fn) fn = getDB().transaction(_fn)
    return fn(...args)
  }
}

function tidyStatement (statement) {
  return statement
    .split('\n')
    .map(line => line.trim())
    .filter(line => !line.startsWith('--'))
    .map(line => line.replaceAll(/  +/g, ' '))
    .join(' ')
    .trim()
}

export function tidy (statements) {
  return statements
    .split(';')
    .map(tidyStatement)
    .filter(Boolean)
    .join(';')
}
