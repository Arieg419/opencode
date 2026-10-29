import { describe, expect } from "bun:test"
import { Deferred, Effect, Fiber, Layer, Schedule, Schema } from "effect"
import { mkdir } from "fs/promises"
import path from "path"
import { Money } from "@opencode/schema/money"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Global } from "@opencode/util/global"
import { makeLocationNode } from "@opencode/util/effect/app-node"
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
import { SessionRunner } from "@opencode/core/session/runner/index"
import { SessionRunnerLLM } from "@opencode/core/session/runner/llm"
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

// Real execution and interruption; only the model turn is replaced.
const runnerNode = makeLocationNode({
  service: SessionRunner.Service,
  layer: Layer.effect(
    SessionRunner.Service,
    Effect.gen(function* () {
      const bus = yield* Bus.Service
      return SessionRunner.Service.of({
        drain: Effect.fn("SubagentQueueTest.drain")(function* ({ sessionID }) {
          if (parents.has(sessionID)) return SessionRunner.DrainResult.Complete()
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
          return SessionRunner.DrainResult.Complete()
        }),
      })
    }),
  ),
  deps: [Bus.node],
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
      SubagentQueue.node,
      KV.node,
      Global.node,
      SessionStore.node,
    ]),
    [
      SessionRunnerLLM.node.replace(runnerNode),
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

// `experimental` goes to global config, `project` to the project's own opencode.json.
const setup = (experimental: Record<string, number>, project?: Record<string, number>) =>
  Effect.gen(function* () {
    started = []
    gates = new Map()
    const dir = yield* Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    )
    const global = yield* Global.Service
    yield* Effect.promise(() => mkdir(global.config, { recursive: true }))
    yield* Effect.promise(() => Bun.write(path.join(global.config, "opencode.json"), JSON.stringify({ experimental })))
    if (project !== undefined)
      yield* Effect.promise(() =>
        Bun.write(path.join(dir.path, "opencode.json"), JSON.stringify({ experimental: project })),
      )
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
    const spawn = (
      sessionID: Session.ID,
      input: { background?: boolean; sessionID?: Session.ID } = {},
      progress?: (update: Record<string, unknown>) => Effect.Effect<void>,
    ) =>
      executeTool(registry, {
        sessionID,
        ...toolIdentity,
        progress,
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
  it.live("only global config sets the cap", () =>
    Effect.gen(function* () {
      const { background } = yield* setup({}, { subagent_concurrency: 1 })
      const results = [yield* background(), yield* background(), yield* background()]
      expect(results.map((result) => result.queued)).toEqual([undefined, undefined, undefined])
      yield* until(() => started.length === 3)
      yield* Effect.forEach(results.map(outputSessionID), finish)
    }).pipe(Effect.scoped),
  )

  it.live("stopping, escaping or removing a queued subagent takes it out of line for good", () =>
    Effect.gen(function* () {
      const { sessions, background } = yield* setup({ subagent_concurrency: 1 })
      const jobs = yield* Job.Service
      const running = outputSessionID(yield* background())
      const [stopped, escaped, removed] = yield* Effect.forEach([1, 2, 3], () =>
        background().pipe(Effect.map(outputSessionID)),
      )
      const queue = yield* SubagentQueue.Service
      yield* sessions.interrupt(stopped!)
      yield* sessions.interrupt(escaped!, { resume: true })
      yield* sessions.remove(removed!)
      // A cancelled background job drops out of the job list once its parent is notified.
      for (const id of [stopped!, escaped!, removed!]) {
        expect((yield* jobs.get(id))?.status).not.toBe("running")
        expect(yield* queue.waiting(id)).toBeUndefined()
      }
      // A second Escape on the cancelled child must not wake its never-sent prompt either.
      yield* sessions.interrupt(escaped!, { resume: true })
      yield* Effect.sleep("50 millis")
      expect(started).toEqual([running])

      yield* finish(running)
      const next = outputSessionID(yield* background())
      yield* until(() => started.includes(next))
      expect(started).toEqual([running, next])
      yield* finish(next)
      yield* jobs.wait({ id: next })
    }).pipe(Effect.scoped),
  )

  it.live("a follow-up to a queued subagent waits in line with it", () =>
    Effect.gen(function* () {
      const { sessions, root, spawn, background } = yield* setup({ subagent_concurrency: 1 })
      const jobs = yield* Job.Service
      const running = outputSessionID(yield* background())
      const queued = outputSessionID(yield* background())
      const steered = yield* spawn(root.id, { sessionID: queued, background: true })
      yield* Effect.sleep("50 millis")
      expect(started).toEqual([running])
      expect(steered.metadata).toEqual({ sessionID: queued, status: "running", queued: 0 })
      expect((yield* sessions.inbox(queued)).map((item) => item.delivery)).toEqual(["queue", "queue"])

      yield* finish(running)
      yield* until(() => started.length === 2)
      expect(started).toEqual([running, queued])
      yield* finish(queued)
      expect((yield* jobs.wait({ id: queued })).info?.status).toBe("completed")
      expect(started).toEqual([running, queued])
    }).pipe(Effect.scoped),
  )

  it.live("aborting a foreground call before it waits cancels its queued subagent", () =>
    Effect.gen(function* () {
      const { root, spawn, background } = yield* setup({ subagent_concurrency: 1 })
      const jobs = yield* Job.Service
      const running = outputSessionID(yield* background())
      const queued = yield* Deferred.make<Session.ID>()
      // Holds the call at its queued progress update, between starting the job and waiting on it.
      const call = yield* spawn(root.id, {}, (update) =>
        update.queued === undefined
          ? Effect.void
          : Deferred.succeed(queued, outputSessionID(update)).pipe(Effect.andThen(Effect.never)),
      ).pipe(Effect.forkChild({ startImmediately: true }))
      const child = yield* Deferred.await(queued)
      yield* Fiber.interrupt(call)
      expect((yield* jobs.get(child))?.status).toBe("cancelled")

      yield* finish(running)
      const next = outputSessionID(yield* background())
      yield* until(() => started.includes(next))
      expect(started).toEqual([running, next])
      yield* finish(next)
      yield* jobs.wait({ id: next })
    }).pipe(Effect.scoped),
  )

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
      // The first child was mid-turn at the restart; the second was still waiting for a slot with its prompt held.
      yield* (yield* SessionStore.Service).claim(children[0]!.id)
      yield* sessions.prompt({ sessionID: children[1]!.id, text: "review", resume: false, delivery: "queue" })
      const notices = (sessionID: Session.ID) =>
        sessions
          .context(sessionID)
          .pipe(Effect.map((messages) => messages.filter((message) => JSON.stringify(message).includes("restarted"))))
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
      expect((yield* notices(children[0]!.id)).length).toBe(1)
      expect(yield* notices(children[1]!.id)).toEqual([])
      yield* finish(children[0]!.id)
      yield* until(() => started.length === 2)
      expect(started).toEqual([children[0]!.id, children[1]!.id])
      expect(yield* notices(children[1]!.id)).toEqual([])
      yield* finish(children[1]!.id)
    }).pipe(Effect.scoped),
  )
})
