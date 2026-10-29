export * as HexDictation from "./hex"

import { execFile } from "node:child_process"
import { homedir } from "node:os"
import util from "node:util"
import { Context, Effect, Fiber, FileSystem, Layer, Option, Path, Result, Schedule, Schema, Scope } from "effect"
import type { HexDictationFinishResult, HexDictationStartResult } from "../../shared/ipc-contract"

const execFilePromise = util.promisify(execFile)

const HEX_BUNDLE_ID = "anomaly.hex"

const HEX_SOURCE = "opencode-desktop"

const POLL = Schedule.spaced("150 millis")

const HEARTBEAT_INTERVAL = "3 seconds"

const DiscoveryDocument = Schema.fromJsonString(
  Schema.Struct({
    port: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65535 })),
    token: Schema.NonEmptyString,
    apiVersion: Schema.Literal("2"),
  }),
)

const HealthResponse = Schema.fromJsonString(Schema.Struct({ apiVersion: Schema.Literal("2") }))

const DictationStarted = Schema.fromJsonString(Schema.Struct({ id: Schema.Int, ownerToken: Schema.NonEmptyString }))

const DictationFinished = Schema.fromJsonString(Schema.Struct({ transcript: Schema.String }))

const DictationLevelEvent = Schema.fromJsonString(Schema.Struct({ rmsDb: Schema.Number, peakDb: Schema.Number }))

const RemoteError = Schema.fromJsonString(Schema.Struct({ code: Schema.optional(Schema.String) }))

const ControlStatusResponse = Schema.fromJsonString(
  Schema.Struct({
    status: Schema.Literal("status"),
    permissions: Schema.Struct({ ready: Schema.Boolean }),
  }),
)

type Endpoint = {
  url: string
  token: string
}

type Lease = {
  endpoint: Endpoint
  id: number
  ownerToken: string
}

type Recording = Lease & { fiber: Fiber.Fiber<void> }

type ControlCommand = { command: "status" } | { command: "window-show"; pane?: "models" }

export type Interface = {
  start(senderID: number, onLevel: (level: number) => void): Effect.Effect<HexDictationStartResult>
  finish(senderID: number): Effect.Effect<HexDictationFinishResult>
  cancel(senderID: number): Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("opencode/desktop/HexDictation") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const scope = yield* Scope.Scope
    const active = new Map<number, Recording>()
    // The latest start of each window. A cancel or a newer start drops it, so a start still waiting on Hex cancels the
    // recording it gets instead of keeping it.
    const starting = new Map<number, object>()

    const readEndpoint = Effect.gen(function* () {
      const raw = yield* fs
        .readFileString(
          path.join(
            process.env.HEX_APPLICATION_SUPPORT_DIR || path.join(homedir(), "Library", "Application Support", "hex"),
            "local-api.json",
          ),
        )
        .pipe(Effect.orElseSucceed(() => ""))

      const discovery = decode(DiscoveryDocument, raw)

      if (!discovery) return undefined
      const endpoint = { url: `http://127.0.0.1:${discovery.port}`, token: discovery.token }
      const health = yield* request(endpoint, "/health", { method: "GET", timeout: 500 })

      return health?.ok && decode(HealthResponse, health.body) ? endpoint : undefined
    })

    // Starts Hex in the background when it is not running, then waits for its local API.
    const ensureRunning = Effect.gen(function* () {
      const running = yield* readEndpoint

      if (running) return running

      const launched = yield* Effect.tryPromise(() => execFilePromise("/usr/bin/open", ["-g", "-b", HEX_BUNDLE_ID])).pipe(
        Effect.as(true),
        Effect.orElseSucceed(() => false),
      )

      if (!launched) return yield* Effect.fail("not-installed" as const)

      return yield* readEndpoint.pipe(
        Effect.flatMap((endpoint) => (endpoint ? Effect.succeed(endpoint) : Effect.fail("not-ready" as const))),
        Effect.retry(POLL.pipe(Schedule.upTo({ duration: "10 seconds" }))),
      )
    })

    const control = (endpoint: Endpoint, command: ControlCommand) =>
      request(endpoint, "/control", { json: { version: 2, ...command }, timeout: 1_500 })

    const release = (lease: Lease) =>
      request(lease.endpoint, `/dictations/${lease.id}/cancel`, { owner: lease.ownerToken, timeout: 2_000 })

    const heartbeat = (lease: Lease) =>
      Effect.sleep(HEARTBEAT_INTERVAL).pipe(
        Effect.andThen(request(lease.endpoint, `/dictations/${lease.id}/heartbeat`, { owner: lease.ownerToken, timeout: 2_000 })),
        // Hex no longer knows the lease.
        Effect.repeat({ until: (response) => response?.status === 401 || response?.status === 404 || response?.status === 409 }),
      )

    const levels = (lease: Lease, onLevel: (level: number) => void) =>
      Effect.tryPromise(async (signal) => {
        const response = await fetch(`${lease.endpoint.url}/dictations/${lease.id}/levels`, {
          headers: headers(lease.endpoint, lease.ownerToken),
          signal,
        })

        if (!response.ok || !response.body) return
        const reader = response.body.pipeThrough(new TextDecoderStream()).getReader()
        let buffer = ""

        while (true) {
          const chunk = await reader.read()

          if (chunk.done) return
          const lines = (buffer + chunk.value).split(/\r?\n/)
          buffer = lines.pop() ?? ""
          lines
            .flatMap((line) => (line.startsWith("data:") ? [decode(DictationLevelEvent, line.slice(5).trim())] : []))
            .forEach((event) => event && onLevel(normalizeLevel(event.rmsDb, event.peakDb)))
        }
      }).pipe(Effect.ignore)

    const take = Effect.fnUntraced(function* (senderID: number) {
      const recording = active.get(senderID)

      if (!recording) return undefined
      active.delete(senderID)
      yield* Fiber.interrupt(recording.fiber)

      return recording
    })

    const stop = Effect.fnUntraced(function* (senderID: number) {
      const recording = yield* take(senderID)

      if (recording) yield* release(recording)
    })

    yield* Effect.addFinalizer(() =>
      Effect.forEach([...active.keys()], stop, { concurrency: "unbounded", discard: true }),
    )

    return Service.of({
      start: Effect.fn("HexDictation.start")(function* (senderID: number, onLevel: (level: number) => void) {
        yield* stop(senderID)
        const ticket = {}
        starting.set(senderID, ticket)
        const running = yield* Effect.result(ensureRunning)

        if (Result.isFailure(running)) return { status: "error", code: running.failure }
        const endpoint = running.success
        const status = yield* control(endpoint, { command: "status" })
        const permissions = status?.ok ? decode(ControlStatusResponse, status.body) : undefined

        if (permissions && !permissions.permissions.ready) {
          yield* control(endpoint, { command: "window-show" })

          return { status: "error", code: "not-ready" }
        }

        // Hex answers 503 while it is still warming up after launch.
        const created = yield* request(endpoint, "/dictations", { json: { source: HEX_SOURCE }, timeout: 3_000 }).pipe(
          Effect.flatMap((response) => (response?.status === 503 ? Effect.fail(response) : Effect.succeed(response))),
          Effect.retry(POLL.pipe(Schedule.upTo({ duration: "5 seconds" }))),
          Effect.catch(Effect.succeed),
        )

        if (!created) return { status: "error", code: "failed" }

        if (created.ok) {
          const started = decode(DictationStarted, created.body)

          if (!started) return { status: "error", code: "failed" }
          const lease = { endpoint, id: started.id, ownerToken: started.ownerToken }

          if (starting.get(senderID) !== ticket) {
            yield* release(lease)

            return { status: "error", code: "failed" }
          }

          starting.delete(senderID)
          active.set(senderID, {
            ...lease,
            fiber: yield* Effect.all([heartbeat(lease), levels(lease, onLevel)], {
              concurrency: "unbounded",
              discard: true,
            }).pipe(Effect.forkIn(scope)),
          })

          return { status: "recording" }
        }

        const code = decode(RemoteError, created.body)?.code

        if (code === "dictation-busy") return { status: "error", code: "busy" }

        if (code === "model-not-ready") {
          yield* control(endpoint, { command: "window-show", pane: "models" })

          return { status: "error", code: "not-ready" }
        }

        return { status: "error", code: "failed", message: code }
      }),
      finish: Effect.fn("HexDictation.finish")(function* (senderID: number) {
        const recording = yield* take(senderID)

        if (!recording) return { status: "error", code: "not-active" }

        const response = yield* request(recording.endpoint, `/dictations/${recording.id}/finish`, {
          owner: recording.ownerToken,
          timeout: 125_000,
        })

        if (!response) return { status: "error", code: "failed" }

        if (response.ok) {
          const transcript = decode(DictationFinished, response.body)?.transcript.trim()

          if (transcript === undefined) return { status: "error", code: "failed" }

          return transcript ? { status: "transcribed", transcript } : { status: "empty" }
        }

        const code = decode(RemoteError, response.body)?.code

        if (code === "capture-discarded") return { status: "empty" }

        return { status: "error", code: "failed", message: code }
      }),
      cancel: Effect.fn("HexDictation.cancel")(function* (senderID: number) {
        starting.delete(senderID)
        yield* stop(senderID)
      }),
    })
  }),
)

/** One local API call; undefined when Hex did not answer. */
function request(
  endpoint: Endpoint,
  path: string,
  options: { method?: "GET" | "POST"; owner?: string; json?: object; timeout: number },
) {
  return Effect.tryPromise(async (signal) => {
    const response = await fetch(`${endpoint.url}${path}`, {
      method: options.method ?? "POST",
      headers: options.json
        ? { ...headers(endpoint, options.owner), "content-type": "application/json" }
        : headers(endpoint, options.owner),
      body: options.json && JSON.stringify(options.json),
      signal: AbortSignal.any([signal, AbortSignal.timeout(options.timeout)]),
    })

    return { status: response.status, ok: response.ok, body: await response.text() }
  }).pipe(Effect.orElseSucceed(() => undefined))
}

function headers(endpoint: Endpoint, owner?: string): Record<string, string> {
  return owner
    ? { authorization: `Bearer ${endpoint.token}`, "x-hex-dictation-token": owner }
    : { authorization: `Bearer ${endpoint.token}` }
}

function decode<A>(schema: Schema.Codec<A, string>, body: string) {
  return Option.getOrUndefined(Schema.decodeUnknownOption(schema)(body))
}

function normalizeLevel(rmsDb: number, peakDb: number) {
  if (!Number.isFinite(rmsDb) || !Number.isFinite(peakDb)) return 0
  const db = rmsDb * 0.65 + peakDb * 0.35
  const linear = Math.max(0, Math.min(1, (db + 48) / 38))

  return Math.pow(linear, 0.8)
}
