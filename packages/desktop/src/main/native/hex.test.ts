import { afterEach, describe, expect, test } from "bun:test"
import { EventEmitter } from "node:events"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { MessageChannel } from "node:worker_threads"
import { NodeFileSystem, NodePath } from "@effect/platform-node"
import type { MessagePortMain, WebContents } from "electron"
import { Layer, ManagedRuntime, Option, Predicate, Schema } from "effect"
import { RpcMessage, RpcServer } from "effect/rpc"
import { HexRpcs } from "../../shared/ipc-rpc/hex"
import { EventRpcs, HexDictationLevel } from "../../shared/ipc-rpc/events"
import { eventHandlers } from "../ipc-handlers/events"
import { hexHandlers } from "../ipc-handlers/hex"
import { bindIpcEvents } from "../ipc-events"
import { IpcPortHandoff, IpcServerProtocolLive } from "../ipc-transport"
import { HexDictation } from "./hex"

const originalPlatform = process.platform

const originalSupportDir = process.env.HEX_APPLICATION_SUPPORT_DIR

const ControlPayload = Schema.fromJsonString(Schema.Struct({ command: Schema.String }))

afterEach(() => {
  Object.defineProperty(process, "platform", { value: originalPlatform })

  if (originalSupportDir === undefined) {
    delete process.env.HEX_APPLICATION_SUPPORT_DIR

    return
  }

  process.env.HEX_APPLICATION_SUPPORT_DIR = originalSupportDir
})

describe("HexDictation IPC", () => {
  test("drives start, finish, and window-destroy cancel against a running Hex local API", async () => {
    Object.defineProperty(process, "platform", { value: "darwin" })
    const dir = await mkdtemp(path.join(tmpdir(), "hex-test-"))
    process.env.HEX_APPLICATION_SUPPORT_DIR = dir

    const requests: Array<{ method: string; path: string; auth: string | null; owner: string | null; body: string }> = []
    let canceledResolve: (() => void) | undefined

    const canceled = new Promise<void>((resolve) => {
      canceledResolve = resolve
    })

    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        const url = new URL(req.url)
        const body = await req.text()
        requests.push({
          method: req.method,
          path: url.pathname,
          auth: req.headers.get("authorization"),
          owner: req.headers.get("x-hex-dictation-token"),
          body,
        })

        if (req.headers.get("authorization") !== "Bearer secret-token") {
          return Response.json({ code: "unauthorized" }, { status: 401 })
        }

        if (req.method === "GET" && url.pathname === "/health") {
          return Response.json({ version: "0.0.0-dev-181", apiVersion: "2" })
        }

        if (req.method === "POST" && url.pathname === "/control") {
          return Response.json({
            version: 2,
            status: "status",
            permissions: { ready: true },
          })
        }

        if (req.method === "POST" && url.pathname === "/dictations") {
          return Response.json({ id: 7, ownerToken: "owner-7", sampleRate: 48_000 }, { status: 201 })
        }

        if (req.method === "GET" && url.pathname === "/dictations/7/levels") {
          if (req.headers.get("x-hex-dictation-token") !== "owner-7") {
            return Response.json({ code: "dictation-not-found" }, { status: 404 })
          }

          return new Response('data: {"rmsDb":-22,"peakDb":-11}\n\n', {
            headers: { "content-type": "text/event-stream" },
          })
        }

        if (req.method === "POST" && url.pathname === "/dictations/7/finish") {
          if (req.headers.get("x-hex-dictation-token") !== "owner-7") {
            return Response.json({ code: "dictation-not-found" }, { status: 404 })
          }

          return Response.json({ transcript: "  Ship the microphone button.  ", durationMs: 1200 })
        }

        if (req.method === "POST" && url.pathname === "/dictations/7/cancel") {
          canceledResolve?.()

          return Response.json({ status: "cancelled" })
        }

        return new Response("not found", { status: 404 })
      },
    })

    await writeFile(
      path.join(dir, "local-api.json"),
      JSON.stringify({
        port: server.port,
        token: "secret-token",
        apiVersion: "2",
        pid: process.pid,
      }),
    )

    const rpcs = HexRpcs.merge(EventRpcs)

    const live = RpcServer.layer(rpcs, { disableFatalDefects: true }).pipe(
      Layer.provide(Layer.merge(hexHandlers, eventHandlers)),
      Layer.provideMerge(IpcServerProtocolLive),
      Layer.provideMerge(HexDictation.layer),
      Layer.provide(Layer.merge(NodeFileSystem.layer, NodePath.layer)),
    )

    const runtime = ManagedRuntime.make(live)

    try {
      const handoff = await runtime.runPromise(IpcPortHandoff)
      const firstSender = createSender(1)
      await runtime.runPromise(bindIpcEvents(1))
      const first = new MessageChannel()
      handoff.bind(firstSender.contents, serverPort(first.port1))
      const receivedLevels: number[] = []
      let levelResolve: (() => void) | undefined

      const gotLevel = new Promise<void>((resolve) => {
        levelResolve = resolve
      })

      subscribeLevels(first.port2, 99, (level) => {
        receivedLevels.push(level)
        levelResolve?.()
      })

      expect((await call(first.port2, 1, "HexDictationStart", null)).exit).toEqual(success({ status: "recording" }))
      await gotLevel
      expect(receivedLevels[0]).toBeGreaterThan(0.5)
      expect(receivedLevels[0]).toBeLessThanOrEqual(1)
      expect(requests.find((item) => item.path === "/dictations")?.body).toBe(
        JSON.stringify({ source: "opencode-desktop" }),
      )

      expect((await call(first.port2, 2, "HexDictationFinish", null)).exit).toEqual(
        success({ status: "transcribed", transcript: "Ship the microphone button." }),
      )

      // Start a second recording and destroy the sender window; the main process must cancel the lease.
      expect((await call(first.port2, 3, "HexDictationStart", null)).exit).toEqual(success({ status: "recording" }))
      firstSender.destroy()
      await canceled
      expect(requests.some((item) => item.path === "/dictations/7/cancel" && item.owner === "owner-7")).toBe(true)

      first.port2.close()
    } finally {
      await runtime.dispose()
      await server.stop(true)
      await rm(dir, { recursive: true, force: true })
    }
  })

  test.each([
    {
      name: "opens Hex window when permissions are not ready",
      permissionsReady: false,
      startStatus: 201,
      startBody: { id: 1, ownerToken: "tok", sampleRate: 16_000 },
      finishStatus: 200,
      finishBody: { transcript: "ignored", durationMs: 0 },
      expectedStart: { status: "error", code: "not-ready" },
      expectWindowShow: true,
    },
    {
      name: "maps dictation-busy conflict to busy",
      permissionsReady: true,
      startStatus: 409,
      startBody: { code: "dictation-busy" },
      finishStatus: 200,
      finishBody: { transcript: "ignored", durationMs: 0 },
      expectedStart: { status: "error", code: "busy" },
      expectWindowShow: false,
    },
    {
      name: "maps capture-discarded finish to empty",
      permissionsReady: true,
      startStatus: 201,
      startBody: { id: 1, ownerToken: "tok", sampleRate: 16_000 },
      finishStatus: 409,
      finishBody: { code: "capture-discarded" },
      expectedStart: { status: "recording" },
      expectedFinish: { status: "empty" },
      expectWindowShow: false,
    },
  ])("$name", async (scenario) => {
    Object.defineProperty(process, "platform", { value: "darwin" })
    const dir = await mkdtemp(path.join(tmpdir(), "hex-case-"))
    process.env.HEX_APPLICATION_SUPPORT_DIR = dir
    const controls: string[] = []

    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        const url = new URL(req.url)

        if (req.method === "GET" && url.pathname === "/health") {
          return Response.json({ version: "0.0.0-dev-181", apiVersion: "2" })
        }

        if (req.method === "POST" && url.pathname === "/control") {
          const payload = Option.getOrUndefined(Schema.decodeUnknownOption(ControlPayload)(await req.text()))

          if (payload) controls.push(payload.command)

          if (payload?.command === "status") {
            return Response.json({
              version: 2,
              status: "status",
              permissions: { ready: scenario.permissionsReady },
            })
          }

          return Response.json({ version: 2, status: "done", message: "ok", changed: true })
        }

        if (req.method === "POST" && url.pathname === "/dictations") {
          return Response.json(scenario.startBody, { status: scenario.startStatus })
        }

        if (req.method === "POST" && url.pathname === "/dictations/1/finish") {
          return Response.json(scenario.finishBody, { status: scenario.finishStatus })
        }

        return new Response("not found", { status: 404 })
      },
    })

    await writeFile(
      path.join(dir, "local-api.json"),
      JSON.stringify({ port: server.port, token: "t", apiVersion: "2", pid: process.pid }),
    )

    const live = RpcServer.layer(HexRpcs, { disableFatalDefects: true }).pipe(
      Layer.provide(hexHandlers),
      Layer.provideMerge(IpcServerProtocolLive),
      Layer.provideMerge(HexDictation.layer),
      Layer.provide(Layer.merge(NodeFileSystem.layer, NodePath.layer)),
    )

    const runtime = ManagedRuntime.make(live)

    try {
      const handoff = await runtime.runPromise(IpcPortHandoff)
      const sender = createSender(9)
      const channel = new MessageChannel()
      handoff.bind(sender.contents, serverPort(channel.port1))

      expect((await call(channel.port2, 1, "HexDictationStart", null)).exit).toEqual(success(scenario.expectedStart))
      expect(controls.includes("window-show")).toBe(scenario.expectWindowShow)

      if ("expectedFinish" in scenario && scenario.expectedFinish) {
        expect((await call(channel.port2, 2, "HexDictationFinish", null)).exit).toEqual(
          success(scenario.expectedFinish),
        )
      }

      channel.port2.close()
    } finally {
      await runtime.dispose()
      await server.stop(true)
      await rm(dir, { recursive: true, force: true })
    }
  })
})

// SAFETY: these are the RPC wire format's plain encoded messages and exits, which the tests post and expect as is.
/* oxlint-disable anti-slop-effect/no-manual-tagged-construction -- see SAFETY above */
function success<A>(value: A) {
  return { _tag: "Success", value } as const
}

function call(port: MessageChannel["port2"], id: number, tag: string, payload: RpcMessage.RequestEncoded["payload"]) {
  const chunks: unknown[] = []

  return new Promise<{ chunks: unknown[]; exit: RpcMessage.ResponseExitEncoded["exit"] }>((resolve) => {
    const onMessage = (message: RpcMessage.FromServerEncoded) => {
      if (!("requestId" in message) || Number(message.requestId) !== id) return

      if (Predicate.isTagged(message, "Chunk")) {
        chunks.push(...message.values)
        port.postMessage({ _tag: "Ack", requestId: message.requestId } satisfies RpcMessage.AckEncoded)

        return
      }

      port.off("message", onMessage)
      resolve({ chunks, exit: message.exit })
    }

    port.on("message", onMessage)
    port.postMessage({ _tag: "Request", id, tag, payload, headers: [] })
  })
}

function subscribeLevels(port: MessageChannel["port2"], id: number, onLevel: (level: number) => void) {
  const decodeLevel = Schema.decodeUnknownOption(HexDictationLevel)
  port.on("message", (message: RpcMessage.FromServerEncoded) => {
    if (!("requestId" in message) || Number(message.requestId) !== id || !Predicate.isTagged(message, "Chunk")) return
    port.postMessage({ _tag: "Ack", requestId: message.requestId } satisfies RpcMessage.AckEncoded)

    for (const item of message.values) {
      const parsed = Option.getOrUndefined(decodeLevel(item))

      if (parsed) onLevel(parsed.level)
    }
  })
  port.postMessage({ _tag: "Request", id, tag: "DesktopEvents", payload: null, headers: [] })
}
/* oxlint-enable anti-slop-effect/no-manual-tagged-construction */

function createSender(id: number) {
  const events = new EventEmitter()
  let destroyed = false

  // SAFETY: the transport and Hex handler read only `id`, `isDestroyed`, and `destroyed` lifecycle listeners.
  // oxlint-disable-next-line anti-slop/no-chained-type-assertions -- see SAFETY above
  const contents = {
    id,
    isDestroyed: () => destroyed,
    once: events.once.bind(events),
    off: events.off.bind(events),
  } as unknown as WebContents

  return {
    contents,
    destroy() {
      destroyed = true
      events.emit("destroyed")
    },
  }
}

function serverPort(port: MessageChannel["port1"]) {
  const listeners = new Map<(event: Electron.MessageEvent) => void, (data: Electron.MessageEvent["data"]) => void>()

  const fake = {
    on(event: string, listener: (event: Electron.MessageEvent) => void) {
      if (event !== "message") {
        port.on(event, listener)

        return
      }

      // SAFETY: the transport reads only the `data` of a message event.
      const wrapped = (data: Electron.MessageEvent["data"]) => listener({ data } as Electron.MessageEvent)
      listeners.set(listener, wrapped)
      port.on("message", wrapped)
    },
    off(event: string, listener: (event: Electron.MessageEvent) => void) {
      if (event !== "message") {
        port.off(event, listener)

        return
      }

      const wrapped = listeners.get(listener)

      if (wrapped) port.off("message", wrapped)
    },
    postMessage: port.postMessage.bind(port),
    start: port.start.bind(port),
    close: port.close.bind(port),
  }

  // SAFETY: the transport uses only these methods of a port.
  // oxlint-disable-next-line anti-slop/no-chained-type-assertions -- see SAFETY above
  return fake as unknown as MessagePortMain
}
