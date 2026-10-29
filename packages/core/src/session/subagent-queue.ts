export * as SubagentQueue from "./subagent-queue.js"

import { Context, Deferred, Effect, Layer, Option } from "effect"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Config } from "../config.js"
import { Instance } from "../instance/service.js"
import { SessionSchema } from "./schema.js"

export type HoldInput = {
  readonly limit: number
  /** Runs before waiting when no slot is free, with how many are waiting ahead. */
  readonly queued?: (ahead: number) => Effect.Effect<void>
  /** Runs once a slot is taken, before the held effect. */
  readonly started?: Effect.Effect<void>
}

export interface Interface {
  /**
   * The cap on a new child of `parent`, or undefined when it runs at once. Only children of
   * top-level sessions are capped: a held nested child could otherwise wait on its own ancestor.
   */
  readonly limit: (parent: SessionSchema.Info) => Effect.Effect<number | undefined>
  /**
   * Runs `effect` once fewer than `limit` held effects run across the server, waiting in FIFO order
   * among waiters with the same limit, and frees the slot when it ends.
   */
  readonly hold: <A, E, R>(input: HoldInput, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SubagentQueue") {}

type Waiter = { readonly limit: number; readonly ready: Deferred.Deferred<void> }

const make = Effect.gen(function* () {
  const instances = yield* Instance.Service
  let running = 0
  const waiting: Waiter[] = []

  // Synchronous bookkeeping first, so the slot count never depends on when the deferreds resolve.
  // Each waiter starts once fewer than its own cap are running, so a small cap at the front never
  // blocks a larger one behind it; waiters with the same cap still start in order.
  const wake = Effect.suspend(() => {
    const woken: Waiter[] = []
    let index = 0
    while (index < waiting.length) {
      if (running >= waiting[index]!.limit) {
        index++
        continue
      }
      running++
      woken.push(...waiting.splice(index, 1))
    }
    return Effect.forEach(woken, (waiter) => Deferred.succeed(waiter.ready, undefined), { discard: true })
  })

  const release = Effect.suspend(() => {
    running--
    return wake
  })

  const leave = (waiter: Waiter) =>
    Effect.suspend(() => {
      const index = waiting.indexOf(waiter)
      if (index === -1) return release
      waiting.splice(index, 1)
      return wake
    })

  const limit: Interface["limit"] = (parent) => {
    if (parent.parentID !== undefined) return Effect.succeed(undefined)
    return Effect.gen(function* () {
      const config = yield* Effect.serviceOption(Config.Service)
      if (Option.isNone(config)) return undefined
      return Config.latest(yield* config.value.entries(), "experimental")?.subagent_concurrency
    }).pipe(
      instances.provide(parent),
      Effect.orElseSucceed(() => undefined),
    )
  }

  const hold: Interface["hold"] = (input, effect) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const waiter: Waiter = { limit: input.limit, ready: yield* Deferred.make<void>() }
        // After every wake no waiter fits under its cap, so starting now never passes one with the same cap.
        if (running < input.limit) running++
        else {
          waiting.push(waiter)
          if (input.queued) yield* input.queued(waiting.length - 1)
          yield* restore(Deferred.await(waiter.ready)).pipe(Effect.onInterrupt(() => leave(waiter)))
        }
        return yield* restore(Effect.andThen(input.started ?? Effect.void, effect)).pipe(Effect.ensuring(release))
      }),
    )

  return Service.of({ limit, hold })
})

export const layer = Layer.effect(Service, make)

export const node = makeGlobalNode({ service: Service, layer, deps: [Instance.node] })
