/** Keeps the keys of `previous` that are still in `current` where they were, then appends the new keys in order. */
export function arrivalOrder(previous: readonly string[], current: readonly string[]) {
  const present = new Set(current)
  const kept = previous.filter((key) => present.has(key))
  const known = new Set(kept)

  return [...kept, ...current.filter((key) => !known.has(key))]
}
