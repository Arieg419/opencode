export * as HexDictation from "./hex"

import { execFile } from "node:child_process"
import { homedir } from "node:os"
import util from "node:util"
import { Clock, Context, Effect, Fiber, FileSystem, Layer, Option, Path, Schema } from "effect"
import type { HexDictationFinishResult, HexDictationStartResult } from "../../shared/ipc-contract"

const execFilePromise = util.promisify(execFile)

const HEX_BUNDLE_ID = "anomaly.hex"

const HEX_SOURCE = "opencode-desktop"

const LAUNCH_TIMEOUT_MS = 10_000

const WARMUP_TIMEOUT_MS = 5_000

const POLL_INTERVAL = "150 millis"

const HEARTBEAT_INTERVAL = "3 seconds"

const DiscoveryDocument = Schema.fromJsonString(
  Schema.Struct({
    port: Schema.Number,
    token: Schema.String,
    apiVersion: Schema.String,
    pid: Schema.Number,
  }),
)

const HealthResponse = Schema.fromJsonString(
  Schema.Struct({
    version: Schema.String,
    apiVersion: Schema.Literal("2"),
  }),
)

const DictationStarted = Schema.fromJsonString(
  Schema.Struct({
    id: Schema.Number,
    ownerToken: Schema.String,
    sampleRate: Schema.Number,
  }),
)

const DictationFinished = Schema.fromJsonString(
  Schema.Struct({
    transcript: Schema.String,
    durationMs: Schema.Number,
  }),
)

const DictationLevelEvent = Schema.fromJsonString(
  Schema.Struct({
    rmsDb: Schema.Number,
    peakDb: Schema.Number,
  }),
)

const RemoteError = Schema.fromJsonString(
  Schema.Struct({
    code: Schema.optional(Schema.String),
  }),
)

const ControlStatusResponse = Schema.fromJsonString(
  Schema.Struct({
    status: Schema.Literal("status"),
    permissions: Schema.Struct({
      ready: Schema.Boolean,
    }),
  }),
)

type Endpoint = {
  url: string
  token: string
}

type ActiveRecording = {
  endpoint: Endpoint
  id: number
  ownerToken: string
  heartbeat: Fiber.Fiber<void>
  levels: Fiber.Fiber<void>
  abortLevels: AbortController
}

export type Interface = {
  start(senderID: number, onLevel?: (level: number) => void): Effect.Effect<HexDictationStartResult>
  finish(senderID: number): Effect.Effect<HexDictationFinishResult>
  cancel(senderID: number): Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("opencode/desktop/HexDictation") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const runFork = Effect.runForkWith(yield* Effect.context())
    const active = new Map<number, ActiveRecording>()

    const supportDirectory = () =>
      process.env.HEX_APPLICATION_SUPPORT_DIR || path.join(homedir(), "Library", "Application Support", "hex")

    const exists = (target: string) => fs.exists(target).pipe(Effect.orElseSucceed(() => false))

    const readEndpoint = Effect.gen(function* () {
      const file = path.join(supportDirectory(), "local-api.json")
      const raw = yield* fs.readFileString(file).pipe(Effect.orElseSucceed(() => undefined))

      if (!raw) return undefined
      const parsed = Option.getOrUndefined(Schema.decodeUnknownOption(DiscoveryDocument)(raw))

      if (
        !parsed ||
        parsed.apiVersion !== "2" ||
        !Number.isInteger(parsed.port) ||
        parsed.port < 1 ||
        parsed.port > 65535 ||
        !Number.isInteger(parsed.pid) ||
        parsed.pid <= 0 ||
        !parsed.token
      ) {
        return undefined
      }

      const url = `http://127.0.0.1:${parsed.port}`

      const healthy = yield* Effect.tryPromise(async () => {
        const response = await fetch(`${url}/health`, {
          headers: { authorization: `Bearer ${parsed.token}` },
          signal: AbortSignal.timeout(500),
        })

        if (!response.ok) return false
        const text = await response.text()

        return Option.isSome(Schema.decodeUnknownOption(HealthResponse)(text))
      }).pipe(Effect.orElseSucceed(() => false))

      return healthy ? ({ url, token: parsed.token } satisfies Endpoint) : undefined
    })

    const findInstalledApp = Effect.gen(function* () {
      const home = process.env.HOME || homedir()
      const candidates = ["/Applications/Hex.app", ...(home ? [path.join(home, "Applications", "Hex.app")] : [])]

      for (const candidate of candidates) {
        if (yield* exists(candidate)) return candidate
      }

      const spotlight = yield* Effect.tryPromise(() =>
        execFilePromise("/usr/bin/mdfind", [`kMDItemCFBundleIdentifier == "${HEX_BUNDLE_ID}"`]),
      ).pipe(Effect.orElseSucceed(() => undefined))

      if (!spotlight) return undefined

      for (const line of spotlight.stdout
        .toString()
        .split(/\r?\n/)
        .map((entry) => entry.trim())
        .filter((entry) => entry.endsWith(".app"))) {
        if (yield* exists(line)) return line
      }

      return undefined
    })

    const ensureRunning = Effect.gen(function* () {
      const running = yield* readEndpoint

      if (running) return { ok: true as const, endpoint: running }
      const bundle = yield* findInstalledApp

      if (!bundle) return { ok: false as const, code: "not-installed" as const }

      const launched = yield* Effect.tryPromise(() => execFilePromise("/usr/bin/open", ["-g", bundle])).pipe(
        Effect.as(true),
        Effect.orElseSucceed(() => false),
      )

      if (!launched) return { ok: false as const, code: "failed" as const }
      const startedAt = yield* Clock.currentTimeMillis

      while ((yield* Clock.currentTimeMillis) - startedAt < LAUNCH_TIMEOUT_MS) {
        const endpoint = yield* readEndpoint

        if (endpoint) return { ok: true as const, endpoint }
        yield* Effect.sleep(POLL_INTERVAL)
      }

      return { ok: false as const, code: "not-ready" as const }
    })

    const checkPermissionsReady = (endpoint: Endpoint) =>
      Effect.tryPromise(async () => {
        const response = await fetch(`${endpoint.url}/control`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${endpoint.token}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({ version: 2, command: "status" }),
          signal: AbortSignal.timeout(1_500),
        })

        if (!response.ok) return true
        const status = Option.getOrUndefined(Schema.decodeUnknownOption(ControlStatusResponse)(await response.text()))

        return status ? status.permissions.ready : true
      }).pipe(Effect.orElseSucceed(() => true))

    const showHexWindow = (endpoint: Endpoint, pane?: string) =>
      Effect.tryPromise(() =>
        fetch(`${endpoint.url}/control`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${endpoint.token}`,
            "content-type": "application/json",
          },
          body: JSON.stringify(pane ? { version: 2, command: "window-show", pane } : { version: 2, command: "window-show" }),
          signal: AbortSignal.timeout(1_500),
        }),
      ).pipe(Effect.ignore)

    const takeActive = (senderID: number) =>
      Effect.gen(function* () {
        const current = active.get(senderID)

        if (!current) return undefined
        active.delete(senderID)
        current.abortLevels.abort()
        yield* Fiber.interrupt(current.heartbeat)
        yield* Fiber.interrupt(current.levels)

        return current
      })

    const cancelActive = (recording: ActiveRecording) =>
      Effect.tryPromise(() =>
        fetch(`${recording.endpoint.url}/dictations/${recording.id}/cancel`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${recording.endpoint.token}`,
            "x-hex-dictation-token": recording.ownerToken,
          },
          signal: AbortSignal.timeout(2_000),
        }),
      ).pipe(Effect.ignore)

    const startHeartbeat = (endpoint: Endpoint, id: number, ownerToken: string) => {
      const loop: Effect.Effect<void> = Effect.sleep(HEARTBEAT_INTERVAL).pipe(
        Effect.andThen(
          Effect.tryPromise(async () => {
            const response = await fetch(`${endpoint.url}/dictations/${id}/heartbeat`, {
              method: "POST",
              headers: {
                authorization: `Bearer ${endpoint.token}`,
                "x-hex-dictation-token": ownerToken,
              },
              signal: AbortSignal.timeout(2_000),
            })

            return response.status
          }).pipe(Effect.orElseSucceed(() => 0)),
        ),
        Effect.flatMap((status) =>
          status === 401 || status === 404 || status === 409 ? Effect.void : loop,
        ),
      )

      return runFork(loop)
    }

    const startLevelStream = (
      endpoint: Endpoint,
      id: number,
      ownerToken: string,
      signal: AbortSignal,
      onLevel?: (level: number) => void,
    ) => {
      if (!onLevel) return runFork(Effect.void)

      return runFork(
        Effect.tryPromise(async () => {
          const response = await fetch(`${endpoint.url}/dictations/${id}/levels`, {
            headers: {
              authorization: `Bearer ${endpoint.token}`,
              "x-hex-dictation-token": ownerToken,
            },
            signal,
          })

          if (!response.ok || !response.body) return
          const reader = response.body.pipeThrough(new TextDecoderStream()).getReader()
          let buffer = ""

          while (!signal.aborted) {
            const chunk = await reader.read()

            if (chunk.done) return
            buffer += chunk.value
            const lines = buffer.split(/\r?\n/)
            buffer = lines.pop() ?? ""

            for (const line of lines) {
              if (!line.startsWith("data:")) continue
              const raw = line.slice(5).trim()

              if (!raw) continue
              const parsed = Option.getOrUndefined(Schema.decodeUnknownOption(DictationLevelEvent)(raw))

              if (parsed) onLevel(normalizeLevel(parsed.rmsDb, parsed.peakDb))
            }
          }
        }).pipe(Effect.ignore),
      )
    }

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const entries = [...active.values()]
        active.clear()
        yield* Effect.forEach(
          entries,
          (entry) => {
            entry.abortLevels.abort()

            return Fiber.interrupt(entry.heartbeat).pipe(
              Effect.andThen(Fiber.interrupt(entry.levels)),
              Effect.andThen(cancelActive(entry)),
            )
          },
          { concurrency: "unbounded", discard: true },
        )
      }),
    )

    return Service.of({
      start: Effect.fn("HexDictation.start")(function* (senderID: number, onLevel?: (level: number) => void) {
        if (process.platform !== "darwin") return { status: "error", code: "unsupported" }
        const previous = yield* takeActive(senderID)

        if (previous) yield* cancelActive(previous)
        const ensured = yield* ensureRunning

        if (!ensured.ok) return { status: "error", code: ensured.code }
        const endpoint = ensured.endpoint
        const ready = yield* checkPermissionsReady(endpoint)

        if (!ready) {
          yield* showHexWindow(endpoint)

          return { status: "error", code: "not-ready" }
        }

        const startedAt = yield* Clock.currentTimeMillis

        while (true) {
          const attempt = yield* Effect.tryPromise(async () => {
            const response = await fetch(`${endpoint.url}/dictations`, {
              method: "POST",
              headers: {
                authorization: `Bearer ${endpoint.token}`,
                "content-type": "application/json",
              },
              body: JSON.stringify({ source: HEX_SOURCE }),
              signal: AbortSignal.timeout(3_000),
            })

            return { status: response.status, ok: response.ok, body: await response.text() }
          }).pipe(Effect.orElseSucceed(() => undefined))

          if (!attempt) return { status: "error", code: "failed" }

          if (attempt.ok) {
            const started = Option.getOrUndefined(Schema.decodeUnknownOption(DictationStarted)(attempt.body))

            if (!started || !Number.isInteger(started.id) || !started.ownerToken) {
              return { status: "error", code: "failed" }
            }

            const abortLevels = new AbortController()
            active.set(senderID, {
              endpoint,
              id: started.id,
              ownerToken: started.ownerToken,
              heartbeat: startHeartbeat(endpoint, started.id, started.ownerToken),
              levels: startLevelStream(endpoint, started.id, started.ownerToken, abortLevels.signal, onLevel),
              abortLevels,
            })

            return { status: "recording" }
          }

          const code = Option.getOrUndefined(Schema.decodeUnknownOption(RemoteError)(attempt.body))?.code

          if (attempt.status === 503 && (yield* Clock.currentTimeMillis) - startedAt < WARMUP_TIMEOUT_MS) {
            yield* Effect.sleep(POLL_INTERVAL)
            continue
          }

          if (code === "dictation-busy") return { status: "error", code: "busy" }

          if (code === "model-not-ready") {
            yield* showHexWindow(endpoint, "models")

            return { status: "error", code: "not-ready" }
          }

          return { status: "error", code: "failed", message: code }
        }
      }),
      finish: Effect.fn("HexDictation.finish")(function* (senderID: number) {
        const recording = yield* takeActive(senderID)

        if (!recording) return { status: "error", code: "not-active" }

        const response = yield* Effect.tryPromise(async () => {
          const reply = await fetch(`${recording.endpoint.url}/dictations/${recording.id}/finish`, {
            method: "POST",
            headers: {
              authorization: `Bearer ${recording.endpoint.token}`,
              "x-hex-dictation-token": recording.ownerToken,
            },
            signal: AbortSignal.timeout(125_000),
          })

          return { ok: reply.ok, body: await reply.text() }
        }).pipe(Effect.orElseSucceed(() => undefined))

        if (!response) return { status: "error", code: "failed" }

        if (response.ok) {
          const decoded = Option.getOrUndefined(Schema.decodeUnknownOption(DictationFinished)(response.body))

          if (!decoded) return { status: "error", code: "failed" }
          const transcript = decoded.transcript.trim()

          return transcript ? { status: "transcribed", transcript } : { status: "empty" }
        }

        const code = Option.getOrUndefined(Schema.decodeUnknownOption(RemoteError)(response.body))?.code

        if (code === "capture-discarded") return { status: "empty" }

        return { status: "error", code: "failed", message: code }
      }),
      cancel: Effect.fn("HexDictation.cancel")(function* (senderID: number) {
        const recording = yield* takeActive(senderID)

        if (recording) yield* cancelActive(recording)
      }),
    })
  }),
)

function normalizeLevel(rmsDb: number, peakDb: number) {
  if (!Number.isFinite(rmsDb) || !Number.isFinite(peakDb)) return 0
  const db = rmsDb * 0.65 + peakDb * 0.35
  const linear = Math.max(0, Math.min(1, (db + 48) / 38))

  return Math.pow(linear, 0.8)
}
