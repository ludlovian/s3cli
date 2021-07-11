export function tidy (sql) {
  return sql
    .split('\n')
    .map(line => line.trim())
    .filter(line => !line.startsWith('--'))
    .map(line => line.replaceAll(/  +/g, ' '))
    .filter(Boolean)
    .join(' ')
    .trim()
    .replaceAll(/; */g, ';')
    .replace(/;$/, '')
}

export function statement (stmt, opts = {}) {
  const { pluck, all, get, db } = opts
  function exec (...args) {
    args = args.map(arg => {
      if (!arg || typeof arg !== 'object') return arg
      return Object.fromEntries(
        Object.entries(arg).map(([k, v]) => [
          k,
          v instanceof Date ? v.toISOString() : v
        ])
      )
    })
    if (stmt.includes(';')) return db().exec(stmt)
    let prep = prepare(stmt, db)
    if (pluck) prep = prep.pluck()
    if (all) return prep.all(...args)
    if (get) return prep.get(...args)
    return prep.run(...args)
  }
  return Object.defineProperties(exec, {
    pluck: { value: () => statement(stmt, { ...opts, pluck: true }) },
    get: { get: () => statement(stmt, { ...opts, get: true }) },
    all: { get: () => statement(stmt, { ...opts, all: true }) }
  })
}

export function transaction (_fn, db) {
  let fn
  return (...args) => {
    if (!fn) fn = db().transaction(_fn)
    return fn(...args)
  }
}

const cache = new Map()
function prepare (stmt, db) {
  let p = cache.get(stmt)
  if (p) return p
  p = db().prepare(stmt)
  cache.set(stmt, p)
  return p
}
