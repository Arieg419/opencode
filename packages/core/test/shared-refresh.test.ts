import { describe, expect } from "bun:test"
import { Deferred, Effect, Fiber, Stream } from "effect"
import { TestClock } from "effect/testing"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { SharedRefresh } from "@opencode/core/shared-refresh"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(SharedRefresh.node))

const elapse = (duration: Parameters<typeof TestClock.adjust>[0]) =>
  TestClock.adjust(duration).pipe(Effect.andThen(Effect.yieldNow))

describe("SharedRefresh", () => {
  it.effect("shares one load per key and refreshes only while a subscriber uses it", () =>
    Effect.gen(function* () {
      const shared = yield* SharedRefresh.Service
      const loads = { a: 0, b: 0 }
      const gate = yield* Deferred.make<void>()
      const load = (key: "a" | "b") => Deferred.await(gate).pipe(Effect.map(() => `${key}${++loads[key]}`))
      const first = yield* shared.subscriber<string>()
      const second = yield* shared.subscriber<string>()
      const other = yield* shared.subscriber<string>()

      const joined = yield* Effect.all(
        [first.use({ key: "a", load: load("a") }), second.use({ key: "a", load: load("a") })],
        { concurrency: "unbounded" },
      ).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.succeed(gate, undefined)
      expect(yield* Fiber.join(joined)).toEqual(["a1", "a1"])
      expect(yield* other.use({ key: "b", load: load("b") })).toBe("b1")
      expect(loads).toEqual({ a: 1, b: 1 })

      yield* elapse("10 minutes")
      expect(loads).toEqual({ a: 2, b: 2 })

      yield* first.release
      yield* elapse("10 minutes")
      expect(loads).toEqual({ a: 3, b: 3 })

      yield* second.release
      yield* elapse("10 minutes")
      expect(loads).toEqual({ a: 3, b: 4 })
      expect(yield* first.use({ key: "a", load: load("a") })).toBe("a4")
    }),
  )

  it.effect("notifies only when a refresh changes the value and keeps it when a refresh fails", () =>
    Effect.gen(function* () {
      const shared = yield* SharedRefresh.Service
      const remote = { version: 1 }
      const notified = { count: 0 }
      const subscriber = yield* shared.subscriber<{ readonly version: number }>()
      yield* subscriber.changes.pipe(
        Stream.runForEach(() => Effect.sync(() => notified.count++)),
        Effect.forkScoped({ startImmediately: true }),
      )
      const options = { key: "models", interval: "1 minute", load: Effect.sync(() => ({ ...remote })) } as const
      expect(yield* subscriber.use(options)).toEqual({ version: 1 })

      yield* elapse("1 minute")
      expect(notified.count).toBe(0)

      remote.version = 2
      yield* elapse("1 minute")
      expect(notified.count).toBe(1)
      expect(yield* subscriber.use(options)).toEqual({ version: 2 })

      yield* subscriber.use({ ...options, load: Effect.fail("offline") })
      yield* elapse("1 minute")
      expect(notified.count).toBe(1)
      expect(yield* subscriber.use({ ...options, load: Effect.fail("offline") })).toEqual({ version: 2 })
    }),
  )
})
