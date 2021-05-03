export const once = fn => {
  function f (...args) {
    if (f.called) return f.value
    f.value = fn(...args)
    f.called = true
    return f.value
  }

  if (fn.name) {
    Object.defineProperty(f, 'name', { value: fn.name, configurable: true })
  }
  return f
}

export function sortBy (name, desc) {
  const fn = typeof name === 'function' ? name : x => x[name]
  const parent = typeof this === 'function' ? this : null
  const m = desc ? -1 : 1
  sortFunc.thenBy = sortBy
  return sortFunc

  function sortFunc (a, b) {
    return (parent && parent(a, b)) || m * compare(a, b, fn)
  }

  function compare (a, b, fn) {
    const va = fn(a)
    const vb = fn(b)
    return va < vb ? -1 : va > vb ? 1 : 0
  }
}
