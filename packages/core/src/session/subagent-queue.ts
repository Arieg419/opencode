export * as SubagentQueue from "./subagent-queue.js"

import { Context, Deferred, Effect, Layer, Option } from "effect"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Config } from "../config.js"
import { Instance } from "../instance/service.js"
import { SessionSchema } from "./schema.js"

export type HoldInput = {
  /** The child whose first turn waits, so interrupting it can take it out of line. */
  readonly sessionID: SessionSchema.ID
  readonly limit: number
  /** Runs once a slot is taken, before the held effect. */
  readonly started?: Effect.Effect<void>
}

export interface Interface {
  /**
   * The server-wide cap on a new child of `parent`, or undefined when it runs at once. Only global config sets it,
   * and only children of top-level sessions are capped: a held nested child could otherwise wait on its own ancestor.
   */
  readonly limit: (parent: SessionSchema.Info) => Effect.Effect<number | undefined>
  /** Runs `effect` once fewer than `limit` held effects run across the server, in FIFO order, and frees the slot when it ends. */
  readonly hold: <A, E, R>(input: HoldInput, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  /** How many wait ahead of a child that is still waiting for a slot, or undefined when it isn't waiting. */
  readonly waiting: (sessionID: SessionSchema.ID) => Effect.Effect<number | undefined>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SubagentQueue") {}

type Waiter = { readonly sessionID: SessionSchema.ID; readonly ready: Deferred.Deferred<void> }

const make = Effect.gen(function* () {
  const instances = yield* Instance.Service
  let running = 0
  const waiting: Waiter[] = []
  const warned = new Set<string>()

  // Synchronous bookkeeping first, so the slot count never depends on when the deferreds resolve.
  const release = (limit: number) =>
    Effect.suspend(() => {
      running--
      const woken: Waiter[] = []
      while (waiting.length > 0 && running < limit) {
        running++
        woken.push(waiting.shift()!)
      }
      return Effect.forEach(woken, (waiter) => Deferred.succeed(waiter.ready, undefined), { discard: true })
    })

  const leave = (waiter: Waiter, limit: number) =>
    Effect.suspend(() => {
      const index = waiting.indexOf(waiter)
      if (index === -1) return release(limit)
      waiting.splice(index, 1)
      return Effect.void
    })

  const limit: Interface["limit"] = (parent) => {
    if (parent.parentID !== undefined) return Effect.succeed(undefined)
    return Effect.gen(function* () {
      const config = yield* Effect.serviceOption(Config.Service)
      if (Option.isNone(config)) return undefined
      const shared = yield* config.value.entries({ project: false })
      for (const entry of yield* config.value.entries()) {
        if (entry.type !== "document" || shared.includes(entry)) continue
        if (entry.info.experimental?.subagent_concurrency === undefined) continue
        const source = entry.path ?? "project config"
        if (warned.has(source)) continue
        warned.add(source)
        yield* Effect.logWarning("experimental.subagent_concurrency is only read from global config", { source })
      }
      return Config.latest(shared, "experimental")?.subagent_concurrency
    }).pipe(
      instances.provide(parent),
      Effect.orElseSucceed(() => undefined),
    )
  }

  const hold: Interface["hold"] = (input, effect) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (waiting.length === 0 && running < input.limit) running++
        else {
          const waiter: Waiter = { sessionID: input.sessionID, ready: yield* Deferred.make<void>() }
          waiting.push(waiter)
          yield* restore(Deferred.await(waiter.ready)).pipe(Effect.onInterrupt(() => leave(waiter, input.limit)))
        }
        return yield* restore(Effect.andThen(input.started ?? Effect.void, effect)).pipe(
          Effect.ensuring(release(input.limit)),
        )
      }),
    )

  const position: Interface["waiting"] = (sessionID) =>
    Effect.sync(() => {
      const index = waiting.findIndex((waiter) => waiter.sessionID === sessionID)
      return index === -1 ? undefined : index
    })

  return Service.of({ limit, hold, waiting: position })
})

export const layer = Layer.effect(Service, make)

export const node = makeGlobalNode({ service: Service, layer, deps: [Instance.node] })
