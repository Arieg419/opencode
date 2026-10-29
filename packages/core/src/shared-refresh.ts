export * as SharedRefresh from "./shared-refresh.js"

import {
  Context,
  Deferred,
  Duration,
  Effect,
  Equal,
  Exit,
  Fiber,
  FiberSet,
  Layer,
  Option,
  Queue,
  Scope,
  Stream,
} from "effect"
import { makeGlobalNode } from "@opencode/util/effect/app-node"

export interface Options<A, E> {
  /** Everything that decides the result, such as integration, credential ID and base URL. Never a token. */
  readonly key: string
  readonly load: Effect.Effect<A, E>
  /** Background refresh period while any subscriber uses the key. Defaults to 10 minutes. */
  readonly interval?: Duration.Input
  /** Decides whether a refreshed value changed. Defaults to deep equality. */
  readonly equals?: (self: A, that: A) => boolean
}

export interface Subscriber<A> {
  /** Moves this subscriber to `key` and returns its last good value. Without one, it joins or starts a load. */
  readonly use: <E>(options: Options<A, E>) => Effect.Effect<A | undefined>
  readonly release: Effect.Effect<void>
  /** Emits when a background refresh changes the value of the key this subscriber uses. */
  readonly changes: Stream.Stream<void>
}

export interface Interface {
  readonly subscriber: <A>() => Effect.Effect<Subscriber<A>, never, Scope.Scope>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SharedRefresh") {}

interface Lease<A> {
  load: Effect.Effect<A, unknown>
  readonly changed: Queue.Queue<void>
}

interface Entry<A> {
  value: Option.Option<A>
  loading: boolean
  notify: boolean
  settled: Deferred.Deferred<void>
  wake: Deferred.Deferred<void>
  readonly leases: Set<Lease<A>>
  fiber?: Fiber.Fiber<void>
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const run = yield* FiberSet.runtime(yield* FiberSet.make<void>())()
    const entries = new Map<string, Entry<unknown>>()

    const load = Effect.fnUntraced(function* <A>(key: string, entry: Entry<A>, equals: (self: A, that: A) => boolean) {
      const lease = Array.from(entry.leases).at(-1)
      if (!lease) return false
      const exit = yield* Effect.exit(lease.load)
      if (Exit.isFailure(exit)) {
        yield* Effect.logWarning("failed to refresh shared value", { key, cause: exit.cause })
        return false
      }
      if (Option.isSome(entry.value) && equals(entry.value.value, exit.value)) return false
      entry.value = Option.some(exit.value)
      return true
    })

    const acquire = <A, E>(options: Options<A, E>, lease: Lease<A>) => {
      // The key decides the value's type, so every lease of one key agrees on it.
      const current = entries.get(options.key) as Entry<A> | undefined
      if (current) {
        current.leases.delete(lease)
        current.leases.add(lease)
        return current
      }
      const equals = options.equals ?? Equal.equals
      const interval = options.interval ?? Duration.minutes(10)
      const entry: Entry<A> = {
        value: Option.none(),
        loading: true,
        notify: false,
        settled: Deferred.makeUnsafe(),
        wake: Deferred.makeUnsafe(),
        leases: new Set([lease]),
      }
      entries.set(options.key, entry)
      entry.fiber = run(
        Effect.gen(function* () {
          const changed = yield* load(options.key, entry, equals)
          const settled = entry.settled
          entry.settled = Deferred.makeUnsafe()
          entry.wake = Deferred.makeUnsafe()
          entry.loading = false
          // Subscribers of the first load read it from `use`.
          if (changed && entry.notify)
            yield* Effect.forEach(entry.leases, (item) => Queue.offer(item.changed, undefined), { discard: true })
          yield* Deferred.succeed(settled, undefined)
          entry.notify = true
          yield* Effect.raceFirst(Effect.sleep(interval), Deferred.await(entry.wake))
          entry.loading = true
        }).pipe(Effect.forever),
      )
      return entry
    }

    const release = (key: string, lease: Lease<unknown>) =>
      Effect.suspend(() => {
        const entry = entries.get(key)
        if (!entry?.leases.delete(lease) || entry.leases.size > 0) return Effect.void
        entries.delete(key)
        return entry.fiber ? Fiber.interrupt(entry.fiber) : Effect.void
      })

    const subscriber = Effect.fnUntraced(function* <A>() {
      const changed = yield* Queue.sliding<void>(1)
      const lease: Lease<A> = { load: Effect.never, changed }
      const state: { key?: string } = {}
      const releaseCurrent = Effect.suspend(() => {
        const key = state.key
        state.key = undefined
        return key === undefined ? Effect.void : release(key, lease)
      })
      yield* Effect.addFinalizer(() => releaseCurrent)
      return {
        use: <E>(options: Options<A, E>) =>
          Effect.gen(function* () {
            lease.load = options.load
            if (state.key !== options.key) yield* releaseCurrent
            const entry = acquire(options, lease)
            state.key = options.key
            if (Option.isSome(entry.value)) return entry.value.value
            const settled = entry.settled
            if (!entry.loading) yield* Deferred.succeed(entry.wake, undefined)
            yield* Deferred.await(settled)
            return Option.getOrUndefined(entry.value)
          }),
        release: releaseCurrent,
        changes: Stream.fromQueue(changed),
      } satisfies Subscriber<A>
    })

    return Service.of({ subscriber })
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [] })
