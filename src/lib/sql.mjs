let db

const SQL = {
  from: sql => statement({ sql: tidy(sql) }),
  attach: _db => (db = _db),
  transaction: fn => transaction(fn)
}

export default SQL

function statement (data) {
  function exec (...args) {
    if (!data.sqlList) data.sqlList = data.sql.split(';')
    if (!data.prepared) data.prepared = []
    const { prepared, sqlList, pluck, raw, get, all } = data
    const n = sqlList.length
    for (let i = 0; i < n - 1; i++) {
      let stmt = prepared[i]
      if (!stmt) stmt = prepared[i] = db.prepare(sqlList[i])
      stmt.run(...args)
    }
    let last = prepared[n - 1]
    if (!last) last = prepared[n - 1] = db.prepare(sqlList[n - 1])
    if (pluck) last = last.pluck()
    if (raw) last = last.raw()
    if (get) return last.get(...args)
    if (all) return last.all(...args)
    return last.run(...args)
  }
  return Object.defineProperties(exec, {
    pluck: { value: () => statement({ ...data, pluck: true }) },
    raw: { value: () => statement({ ...data, raw: true }) },
    get: { get: () => statement({ ...data, get: true }) },
    all: { get: () => statement({ ...data, all: true }) }
  })
}

function transaction (_fn) {
  let fn
  return (...args) => {
    if (!fn) fn = db.transaction(_fn)
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
