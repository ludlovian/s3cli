// generic matcher
//
// takes a range of streams (as async generators) and matches on keys,
// yielding a tuple of entries
//
// Key is specified as a picking function, or a string (to use that
// property)
//
// If a key is not found in all streams, then the missing entry in the
// tuple will be falsy.
//
// It works best if everything is (mostly) sorted, but can cope with things
// out of order.

export default async function * match (selectKey, ...sources) {
  selectKey = makeSelector(selectKey)

  // we will store out-of-order things in maps just in case they arrive
  // later. Once we have created a full entry we can yield it out.
  //
  // Once we get to the end, we know we have a collection of partial
  // entries.
  const found = sources.map(() => new Map())

  // read the first item of each source to prefill our head vector
  let heads = await readAll(sources)

  while (true) {
    // If all the heads are off the end, then we have run out of new
    // data, and we exit the loop, prior to cleaning up partials
    //
    if (heads.every(v => !v)) break

    // if all the keys are the same, then we are delightfully in sync
    // We hope this is the most common scenario. We can just yield out
    // the current head tuple and move on
    if (allHaveSameKey(heads)) {
      yield heads
      heads = await readAll(sources)
      continue
    }

    // Alas we are not in sync. So let's find the earliest key, and
    // pick all the sources on that key
    const currKey = findEarliestKey(heads)
    const matches = heads.map(v => v && selectKey(v) === currKey)

    // if we have found all the missing ones already, then we are
    // okay again. We can extract the missing saved ones, yield the
    // full tuple and move on
    if (matches.every((matched, ix) => matched || found[ix].has(currKey))) {
      const current = heads.map((v, ix) => {
        if (!matches[ix]) {
          v = found[ix].get(currKey)
          found[ix].delete(currKey)
        }
        return v
      })
      yield current
      heads = await readSome(sources, heads, matches)
      continue
    }

    // Sadly, we do not have enough to complete a full entry, so we
    // store away the ones we have, and advance and go around again
    heads.forEach((v, ix) => {
      if (matches[ix]) found[ix].set(currKey, v)
    })
    heads = await readSome(sources, heads, matches)
  }

  // we have finished all sources, and what we have left in the found
  // maps is a collection of partial entries. So we just go through these
  // in any old order, assembling partials as best we can.

  // first lets get a unique list of all the keys
  const keys = found.reduce(
    (keys, map) => new Set([...keys, ...map.keys()]),
    new Set()
  )

  // and for each key, we assemble the partial entry and yield
  for (const key of keys) {
    const current = heads.map((v, ix) =>
      found[ix].has(key) ? found[ix].get(key) : undefined
    )
    yield current
  }

  // and we're done.

  function allHaveSameKey (vals) {
    const keys = vals.map(v => (v ? selectKey(v) : null))
    return keys.every(k => k === keys[0])
  }

  function findEarliestKey (vals) {
    return vals.reduce((earliest, v) => {
      if (!v) return earliest
      const k = selectKey(v)
      return !earliest || k < earliest ? k : earliest
    }, null)
  }

  function readSome (gens, curr, matches) {
    return Promise.all(
      gens.map((gen, ix) => (matches[ix] ? readItem(gen) : curr[ix]))
    )
  }

  function readAll (gens) {
    return Promise.all(gens.map(readItem))
  }

  function readItem (gen) {
    return gen.next().then(v => (v.done ? undefined : v.value))
  }

  function makeSelector (sel) {
    return typeof sel === 'function' ? sel : x => x[sel]
  }
}
