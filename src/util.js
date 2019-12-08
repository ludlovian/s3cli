'use strict'

export async function retry (fn, { count = 5, backoff = 1 } = {}) {
  let attempt = 1
  while (true) {
    try {
      return fn()
    } catch (err) {
      if (attempt++ > count) throw err
      console.error(
        [
          '\n',
          err.message,
          `Waiting ${backoff}s before #${attempt} of ${count} ...`
        ].join('')
      )
      await delay(backoff * 1e3)
      backoff = backoff * 2
    }
  }
}

function delay (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export const once = fn => {
  let called = false
  let value
  return (...args) => {
    if (called) return value
    called = true
    value = fn(...args)
    return value
  }
}

export function wrap (fn) {
  return (...args) =>
    Promise.resolve(fn(...args)).catch(err => {
      console.error(err)
      process.exit(1)
    })
}

export const time = () => {}
export const size = () => {}
export const comma = () => {}
