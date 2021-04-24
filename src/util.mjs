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
