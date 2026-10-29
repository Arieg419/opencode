export * as SubagentQueue from "./subagent-queue.js"

import path from "path"
import { Context, Deferred, Effect, Layer } from "effect"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { FSUtil } from "@opencode/util/fs-util"
import { Global } from "@opencode/util/global"
import { Config } from "../config.js"
import { ConfigDiscovery } from "../config/discovery.js"
import { ConfigVariable } from "../config/variable.js"
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
   * The server-wide cap on a new child of `parent`, or undefined when it runs at once. Only server-wide config sets
   * it (global, explicit file, inline content, managed), and only children of top-level sessions are capped: a held
   * nested child could otherwise wait on its own ancestor.
   */
  readonly limit: (parent: SessionSchema.Info) => Effect.Effect<number | undefined>
  /** The config files the cap is read from, lowest priority first. */
  readonly files: ReadonlyArray<string>
  /** Runs `effect` once fewer than `limit` held effects run across the server, in FIFO order, and frees the slot when it ends. */
  readonly hold: <A, E, R>(input: HoldInput, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  /** How many wait ahead of a child that is still waiting for a slot, or undefined when it isn't waiting. */
  readonly waiting: (sessionID: SessionSchema.ID) => Effect.Effect<number | undefined>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SubagentQueue") {}

type Waiter = { readonly sessionID: SessionSchema.ID; readonly ready: Deferred.Deferred<void> }

/** The server's own config sources beside the global directory, as passed to `Config.configured`. */
export type Options = Pick<Config.Options, "file" | "content">

const CONTENT = "OPENCODE_CONFIG_CONTENT"

const make = Effect.fn("SubagentQueue.make")(function* (options?: Options) {
  const fs = yield* FSUtil.Service
  const global = yield* Global.Service
  const inDirectory = (directory: string) => ConfigDiscovery.names.map((name) => path.join(directory, name))
  // The server-wide sources in Config's priority order: global directory, explicit file, inline content, then
  // managed system config, which overrides everything.
  const globalFiles = inDirectory(global.config)
  const explicit = options?.file ? [path.resolve(options.file)] : []
  const system = inDirectory(ConfigDiscovery.systemDirectory())
  const files = [...globalFiles, ...explicit, ...system]
  const sources = [...globalFiles, ...explicit, ...(options?.content !== undefined ? [CONTENT] : []), ...system]
  // Each source is parsed again only when its text changes, so a rejected document warns once per edit.
  const parsed = new Map<string, { readonly text: string; readonly cap: number | undefined }>()
  const read = (source: string) =>
    Effect.gen(function* () {
      const text = source === CONTENT ? options?.content : yield* fs.readFileStringSafe(source)
      if (text === undefined) return undefined
      const cached = parsed.get(source)
      if (cached?.text === text) return cached.cap
      const substituted = yield* ConfigVariable.substitute(
        source === CONTENT
          ? { type: "virtual", source, dir: global.config, text }
          : { type: "path", path: source, text },
      )
      const cap = (yield* Config.parseInfo(substituted, source))?.experimental?.subagent_concurrency
      parsed.set(source, { text, cap })
      return cap
    }).pipe(
      Effect.provideService(FSUtil.Service, fs),
      Effect.catch((error) =>
        Effect.logWarning("failed to read subagent_concurrency", { source, error }).pipe(Effect.as(undefined)),
      ),
    )
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

  // Read straight from the server-wide sources, so checking the cap never boots a project.
  const limit: Interface["limit"] = (parent) => {
    if (parent.parentID !== undefined) return Effect.succeed(undefined)
    return Effect.forEach(sources, read).pipe(Effect.map((caps) => caps.findLast((cap) => cap !== undefined)))
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

export const layer = (options?: Options) => Layer.effect(Service, make(options))

export function configured(options?: Options) {
  return makeGlobalNode({ service: Service, layer: layer(options), deps: [FSUtil.node, Global.node] })
}

export const node = configured()
