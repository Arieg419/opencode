export * as SubagentQueue from "./subagent-queue.js"

import path from "path"
import { Context, Deferred, Effect, Layer, Option, Schema } from "effect"
import { parse } from "jsonc-parser"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { FSUtil } from "@opencode/util/fs-util"
import { Global } from "@opencode/util/global"
import { ConfigDiscovery } from "../config/discovery.js"
import { PositiveInt } from "../schema.js"
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
  /** The global config files the cap is read from, lowest priority first. */
  readonly files: ReadonlyArray<string>
  /** Runs `effect` once fewer than `limit` held effects run across the server, in FIFO order, and frees the slot when it ends. */
  readonly hold: <A, E, R>(input: HoldInput, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  /** How many wait ahead of a child that is still waiting for a slot, or undefined when it isn't waiting. */
  readonly waiting: (sessionID: SessionSchema.ID) => Effect.Effect<number | undefined>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SubagentQueue") {}

type Waiter = { readonly sessionID: SessionSchema.ID; readonly ready: Deferred.Deferred<void> }

const decodeCap = Schema.decodeUnknownOption(
  Schema.Struct({ experimental: Schema.Struct({ subagent_concurrency: PositiveInt }) }),
)

const make = Effect.gen(function* () {
  const fs = yield* FSUtil.Service
  const global = yield* Global.Service
  const files = ConfigDiscovery.names.map((name) => path.join(global.config, name))
  let running = 0
  const waiting: Waiter[] = []

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

  // Read straight from the global config files, so checking the cap never boots a project.
  const limit: Interface["limit"] = (parent) => {
    if (parent.parentID !== undefined) return Effect.succeed(undefined)
    return Effect.forEach(files, (file) =>
      fs.readFileStringSafe(file).pipe(
        Effect.map((text) =>
          text === undefined
            ? undefined
            : Option.getOrUndefined(decodeCap(parse(text, [], { allowTrailingComma: true })))?.experimental
                .subagent_concurrency,
        ),
        Effect.orElseSucceed(() => undefined),
      ),
    ).pipe(Effect.map((caps) => caps.findLast((cap) => cap !== undefined)))
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

  return Service.of({ limit, files, hold, waiting: position })
})

export const layer = Layer.effect(Service, make)

export const node = makeGlobalNode({ service: Service, layer, deps: [FSUtil.node, Global.node] })
