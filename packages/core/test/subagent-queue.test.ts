import { describe, expect } from "bun:test"
import { Effect, Fiber, Layer, Schedule, Schema } from "effect"
import path from "path"
import { Money } from "@opencode/schema/money"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Global } from "@opencode/util/global"
import { makeGlobalNode, makeLocationNode } from "@opencode/util/effect/app-node"
import { Database } from "@opencode/core/database/database"
import { Bus } from "@opencode/core/bus"
import { Config } from "@opencode/core/config"
import { Location } from "@opencode/core/location"
import { Model } from "@opencode/core/model"
import { Provider } from "@opencode/core/provider"
import { AbsolutePath } from "@opencode/core/schema"
import { Agent } from "@opencode/core/agent"
import { Job } from "@opencode/core/job"
import { KV } from "@opencode/core/kv"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { Session } from "@opencode/core/session"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionRestart } from "@opencode/core/session/execution/restart"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionStore } from "@opencode/core/session/store"
import { SubagentJob } from "@opencode/core/session/subagent-job"
import { SubagentQueue } from "@opencode/core/session/subagent-queue"
import { Plugin } from "@opencode/core/plugin"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { PluginSupervisor } from "@opencode/core/plugin/supervisor"
import { Permission } from "@opencode/core/permission"
import { SubagentTool } from "@opencode/core/tool/plugin/subagent"
import { Tool } from "@opencode/core/tool"
import { tmpdir } from "./fixture/tmpdir"
import { tempGlobalLayer } from "./fixture/global"
import { offlineModels } from "./fixture/models"
import { testEffect } from "./lib/effect"
import { executeTool, registerToolPlugin, toolIdentity } from "./lib/tool"

const childModel = Model.Ref.make({ id: Model.ID.make("child"), providerID: Provider.ID.make("test") })
const tokens = { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } }

// Every child turn start is recorded and holds until the test finishes it. Parents are ignored.
let parents = new Set<Session.ID>()
let started: Session.ID[] = []
let gates = new Map<Session.ID, PromiseWithResolvers<void>>()
const gate = (sessionID: Session.ID) => {
  const existing = gates.get(sessionID)
  if (existing) return existing
  const created = Promise.withResolvers<void>()
  gates.set(sessionID, created)
  return created
}
const finish = (sessionID: Session.ID) => Effect.sync(() => gate(sessionID).resolve())

const executionNode = makeGlobalNode({
  service: SessionExecution.Service,
  layer: Layer.effect(
    SessionExecution.Service,
    Effect.gen(function* () {
      const bus = yield* Bus.Service
      const resume = Effect.fn("SubagentQueueTest.resume")(function* (sessionID: Session.ID) {
        if (parents.has(sessionID)) return
        started.push(sessionID)
        yield* Effect.promise(() => gate(sessionID).promise)
        const assistantMessageID = SessionMessage.ID.create()
        yield* bus.publish(SessionEvent.Step.Started, {
          sessionID,
          assistantMessageID,
          agent: Agent.ID.make("reviewer"),
          model: childModel,
          started: 0,
        })
        yield* bus.publish(SessionEvent.Step.Ended, {
          sessionID,
          assistantMessageID,
          finish: "stop",
          cost: Money.USD.zero,
          tokens,
        })
      })
      return SessionExecution.Service.of({
        active: Effect.succeed(new Set()),
        isActive: () => Effect.succeed(false),
        resume,
        wake: () => Effect.void,
        interrupt: () => Effect.succeed(false),
        awaitIdle: () => Effect.void,
      })
    }),
  ),
  deps: [Bus.node, SessionStore.node],
})

const subagentPluginSupervisor = makeLocationNode({
  name: "test/subagent-queue-plugins",
  layer: Layer.effectDiscard(
    Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      yield* registerToolPlugin(SubagentTool.Plugin, {}, (name, callback) => hooks.register("tool", name, callback))
    }),
  ),
  deps: [
    Agent.node,
    Config.node,
    Model.node,
    Permission.node,
    Session.node,
    Job.node,
    SubagentJob.node,
    SubagentQueue.node,
    Tool.node,
    PluginHooks.node,
  ],
})

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      Bus.node,
      Job.node,
      Session.node,
      SessionExecution.node,
      LocationServiceMap.node,
      SessionRestart.node,
      KV.node,
    ]),
    [
      SessionExecution.node.replace(executionNode),
      Global.node.replace(tempGlobalLayer),
      offlineModels,
      PluginSupervisor.node.replace(subagentPluginSupervisor),
    ],
  ),
)

const outputSessionID = (value: unknown) =>
  Schema.decodeUnknownSync(Schema.Struct({ sessionID: Session.ID }))(value).sessionID

const until = (check: () => boolean) =>
  Effect.sync(check).pipe(
    Effect.repeat({ until: (done) => done, schedule: Schedule.spaced("5 millis") }),
    Effect.timeout("2 seconds"),
  )

const setup = (experimental: Record<string, number>) =>
  Effect.gen(function* () {
    started = []
    gates = new Map()
    const dir = yield* Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    )
    yield* Effect.promise(() => Bun.write(path.join(dir.path, "opencode.json"), JSON.stringify({ experimental })))
    const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
    const sessions = yield* Session.Service
    const root = yield* sessions.create({ location })
    parents = new Set([root.id])
    const locations = yield* LocationServiceMap.Service
    yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))
    yield* Agent.Service.use((agents) =>
      agents.transform((editor) => {
        editor.update(toolIdentity.agent, (agent) => {
          agent.mode = "primary"
          agent.permissions.push({ action: "*", resource: "*", effect: "allow" })
        })
        editor.update(Agent.ID.make("reviewer"), (agent) => {
          agent.mode = "subagent"
          agent.model = childModel
        })
      }),
    ).pipe(Effect.provide(locations.get(location)))
    const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
    let calls = 0
    const spawn = (sessionID: Session.ID, input: { background?: boolean; sessionID?: Session.ID } = {}) =>
      executeTool(registry, {
        sessionID,
        ...toolIdentity,
        call: {
          type: "tool-call",
          id: `call-${++calls}`,
          name: SubagentTool.name,
          input: { agent: "reviewer", description: "review", prompt: "review", ...input },
        },
      })
    const background = (sessionID = root.id) =>
      spawn(sessionID, { background: true }).pipe(Effect.map((result) => result.metadata as Record<string, unknown>))
    return { sessions, root, spawn, background }
  })

describe("SubagentQueue", () => {
  it.live("holds new subagents past the cap and starts them in FIFO order", () =>
    Effect.gen(function* () {
      const { background } = yield* setup({ subagent_concurrency: 2 })
      const results = [yield* background(), yield* background(), yield* background(), yield* background()]
      const [a, b, c, d] = results.map(outputSessionID)
      expect(results.map((result) => result.queued)).toEqual([undefined, undefined, 0, 1])
      yield* until(() => started.length === 2)
      expect(started).toEqual([a!, b!])

      yield* finish(a!)
      yield* until(() => started.length === 3)
      expect(started).toEqual([a!, b!, c!])

      yield* finish(b!)
      yield* until(() => started.length === 4)
      expect(started).toEqual([a!, b!, c!, d!])
      yield* Effect.forEach([c!, d!], finish)
      const jobs = yield* Job.Service
      yield* Effect.forEach([a!, b!, c!, d!], (id) => jobs.wait({ id }))
    }).pipe(Effect.scoped),
  )

  it.live("never holds a steer of an existing child", () =>
    Effect.gen(function* () {
      const { sessions, root, spawn, background } = yield* setup({ subagent_concurrency: 1 })
      const running = outputSessionID(yield* background())
      const existing = yield* sessions.create({
        parentID: root.id,
        title: "existing",
        agent: Agent.ID.make("reviewer"),
      })
      const steered = yield* spawn(root.id, { sessionID: existing.id, background: true })
      expect(steered.metadata).toEqual({ sessionID: existing.id, status: "running" })
      yield* until(() => started.includes(existing.id))
      yield* Effect.forEach([running, existing.id], finish)
    }).pipe(Effect.scoped),
  )

  it.live("never holds a nested subagent", () =>
    Effect.gen(function* () {
      const { sessions, root, background } = yield* setup({ subagent_concurrency: 1, subagent_depth: 2 })
      const running = outputSessionID(yield* background())
      const parent = yield* sessions.create({ parentID: root.id, title: "parent" })
      parents.add(parent.id)
      const nested = yield* background(parent.id)
      expect(nested.queued).toBeUndefined()
      yield* until(() => started.includes(outputSessionID(nested)))
      yield* Effect.forEach([running, outputSessionID(nested)], finish)
    }).pipe(Effect.scoped),
  )

  it.live("cancelling or aborting a queued subagent never starts its turn", () =>
    Effect.gen(function* () {
      const { root, spawn, background } = yield* setup({ subagent_concurrency: 1 })
      const jobs = yield* Job.Service
      const sessions = yield* Session.Service
      const running = outputSessionID(yield* background())
      const cancelled = yield* background()
      expect(cancelled.queued).toBe(0)
      expect((yield* jobs.cancel(outputSessionID(cancelled)))?.status).toBe("cancelled")

      // A foreground call waits for its slot; aborting it leaves the queue too.
      const foreground = yield* spawn(root.id).pipe(Effect.forkChild({ startImmediately: true }))
      const children = sessions.list({ parentID: root.id }).pipe(Effect.map((page) => page.data))
      const waiting = yield* children.pipe(
        Effect.repeat({ until: (data) => data.length === 3, schedule: Schedule.spaced("5 millis") }),
        Effect.map((data) => data.find((child) => child.id !== running && child.id !== outputSessionID(cancelled))!),
      )
      yield* jobs
        .get(waiting.id)
        .pipe(Effect.repeat({ until: (info) => info !== undefined, schedule: Schedule.spaced("5 millis") }))
      yield* Effect.sleep("50 millis")
      yield* Fiber.interrupt(foreground)
      expect((yield* jobs.get(waiting.id))?.status).toBe("cancelled")

      yield* finish(running)
      const next = outputSessionID(yield* background())
      yield* until(() => started.includes(next))
      expect(started).toEqual([running, next])
      yield* finish(next)
      yield* jobs.wait({ id: next })
    }).pipe(Effect.scoped),
  )

  it.live("runs every subagent at once without a configured cap", () =>
    Effect.gen(function* () {
      const { background } = yield* setup({})
      const results = [yield* background(), yield* background(), yield* background()]
      expect(results.map((result) => result.queued)).toEqual([undefined, undefined, undefined])
      yield* until(() => started.length === 3)
      yield* Effect.forEach(results.map(outputSessionID), finish)
    }).pipe(Effect.scoped),
  )

  it.live("restart recovery lines recovered subagents up behind the cap", () =>
    Effect.gen(function* () {
      const { sessions, root } = yield* setup({ subagent_concurrency: 1 })
      const kv = yield* KV.Service
      const children = yield* Effect.forEach([1, 2], (n) =>
        sessions.create({ parentID: root.id, title: `recovered ${n}`, agent: Agent.ID.make("reviewer") }),
      )
      yield* Effect.forEach(children, (child) => {
        const notificationID = SessionMessage.ID.create()
        return kv.set(`job.background/${notificationID}`, {
          id: child.id,
          notificationID,
          recovery: {
            kind: "subagent" as const,
            parentSessionID: root.id,
            childSessionID: child.id,
            agent: "reviewer",
            description: "recovered",
          },
          status: "running" as const,
        })
      })
      yield* (yield* SessionRestart.Service).resumeSuspendedSessions
      yield* until(() => started.length === 1)
      expect(started).toEqual([children[0]!.id])
      yield* finish(children[0]!.id)
      yield* until(() => started.length === 2)
      expect(started).toEqual([children[0]!.id, children[1]!.id])
      yield* finish(children[1]!.id)
    }).pipe(Effect.scoped),
  )
})
