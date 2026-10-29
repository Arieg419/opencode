import { describe, expect } from "bun:test"
import type { FileSystem } from "@opencode/core/filesystem"
import { DateTime, Effect, Exit, Fiber, Layer, LayerMap, Queue, Schema, Stream } from "effect"
import { TestClock } from "effect/testing"
import { eq } from "drizzle-orm"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { Permission } from "@opencode/core/permission"
import { PermissionSaved } from "@opencode/core/permission/saved"
import { Tool } from "@opencode/core/tool"
import { McpTool } from "@opencode/core/tool/mcp"
import { Instance } from "@opencode/core/instance/service"
import { Location } from "@opencode/core/location"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath } from "@opencode/core/schema"
import { Schedule } from "@opencode/core/schedule"
import { ScheduleTable } from "@opencode/core/schedule/sql"
import { Session } from "@opencode/core/session"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionInbox } from "@opencode/core/session/inbox"
import { SessionInboxTable, SessionTable } from "@opencode/core/session/sql"
import { SessionStore } from "@opencode/core/session/store"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import type { LocationServices } from "@opencode/core/location-services"
import { Plugin } from "@opencode/core/plugin"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Snapshot } from "@opencode/core/snapshot"
import { Skill } from "@opencode/core/skill"
import { Config } from "@opencode/core/config"
import { Document, Info } from "@opencode/schema/config"
import { ConfigExperimental } from "@opencode/schema/config/experimental"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"

const enabledDirectory = "/enabled"
const disabledDirectory = "/disabled"
const toggleDirectory = "/toggle"

const wakes: Session.ID[] = []
/** Runs inside the next wake, which happens while a schedule is firing. */
let duringWake: Effect.Effect<void> = Effect.void
const execution = Layer.succeed(
  SessionExecution.Service,
  SessionExecution.Service.of({
    active: Effect.sync(() => new Set()),
    isActive: () => Effect.succeed(false),
    resume: () => Effect.void,
    interrupt: () => Effect.succeed(false),
    wake: (sessionID) =>
      Effect.suspend(() => {
        wakes.push(sessionID)
        const effect = duringWake
        duringWake = Effect.void
        return effect
      }),
    awaitIdle: () => Effect.void,
  }),
)
const enabledEntries = [
  new Document({
    type: "document",
    info: new Info({
      timezone: "America/New_York",
      experimental: new ConfigExperimental.Info({ schedules: true }),
    }),
  }),
]
const enabled = Config.testLayer(enabledEntries)
/** A location whose flag the test turns on, counting every config read. */
const toggle = { on: false, reads: 0 }
const toggled = Layer.succeed(
  Config.Service,
  Config.Service.of({
    entries: () =>
      Effect.sync(() => {
        toggle.reads++
        return toggle.on ? enabledEntries : []
      }),
    changes: () => Stream.empty,
  }),
)
/** What the check tools see and do; reset by setup. */
const slack = { replies: 0, reads: 0, posts: 0, exports: 0, failing: false }
/** The default agent's permission rules; set after setup. */
let agentRules: Permission.Ruleset = []
// Reads assert their permission the way MCP tools do; with no rules it would ask.
const checkTools = Layer.effectDiscard(
  Effect.gen(function* () {
    const tools = yield* Tool.Service
    const agents = yield* Agent.Service
    const permission = yield* Permission.Service
    yield* agents.transform((editor) =>
      editor.update(Agent.defaultID, (agent) => {
        agent.permissions = [...agentRules]
      }),
    )
    yield* tools.transform((editor) => {
      editor.add({
        name: "read_thread",
        description: "Read the thread",
        input: Schema.Struct({}),
        output: Schema.Struct({ replies: Schema.Number }),
        options: { namespace: "slack" },
        execute: (_input, context) =>
          permission
            .assert({
              action: "slack_read_thread",
              resources: ["*"],
              sessionID: context.sessionID,
              agent: context.agent,
            })
            .pipe(
              Effect.orDie,
              Effect.flatMap(() => {
                if (slack.failing) return Effect.fail(new Tool.Error({ message: "Slack is down" }))
                slack.reads++
                return Effect.succeed({ output: { replies: slack.replies } })
              }),
            ),
      })
      // Needs a second permission besides its own, which a check was never approved for.
      editor.add({
        name: "export_thread",
        description: "Export the thread to a file",
        input: Schema.Struct({}),
        options: { namespace: "slack" },
        execute: (_input, context) =>
          permission
            .assert({ action: "file_write", resources: ["*"], sessionID: context.sessionID, agent: context.agent })
            .pipe(
              Effect.orDie,
              Effect.map(() => {
                slack.exports++
                return { content: "exported" }
              }),
            ),
      })
      editor.add({
        name: "post",
        description: "Post a reply",
        input: Schema.Struct({}),
        options: { namespace: "slack" },
        execute: () =>
          Effect.sync(() => {
            slack.posts++
            return { content: "posted" }
          }),
      })
    })
  }),
)
const locations = makeGlobalNode({
  service: LocationServiceMap.Service,
  layer: Layer.effect(
    LocationServiceMap.Service,
    Effect.gen(function* () {
      const bus = yield* Bus.Service
      const store = yield* SessionStore.Service
      const saved = yield* PermissionSaved.Service
      return yield* LayerMap.make(
        (ref: Location.Ref) =>
          // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
          Layer.mergeAll(
            ref.directory === enabledDirectory ? enabled : ref.directory === toggleDirectory ? toggled : Layer.empty,
            checkTools.pipe(
              Layer.provideMerge(
                LayerNode.compile(
                  LayerNode.group([PluginHooks.node, Skill.node, Tool.node, Agent.node, Permission.node]),
                  {
                    replacements: [
                      Bus.node.replace(Layer.succeed(Bus.Service, bus)),
                      SessionStore.node.replace(Layer.succeed(SessionStore.Service, store)),
                      PermissionSaved.node.replace(Layer.succeed(PermissionSaved.Service, saved)),
                      Location.node.replace(Layer.succeed(Location.Service, location(ref))),
                    ],
                  },
                ),
              ),
            ),
            Layer.mock(McpTool.Service, { flush: Effect.void }),
            Layer.mock(Snapshot.Service, { capture: () => Effect.undefined, restore: () => Effect.void }),
            Layer.mock(Plugin.Service, { awaitActivation: Effect.void }),
          ).pipe(Layer.fresh) as unknown as Layer.Layer<LocationServices, FileSystem.DirectoryNotFoundError>,
      )
    }),
  ),
  deps: [Bus.node, SessionStore.node, PermissionSaved.node],
})
const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      Bus.node,
      SessionProjector.node,
      SessionStore.node,
      Session.node,
      Schedule.node,
      Instance.node,
    ]),
    [
      Bus.node.replace(Bus.configured({ persist: true })),
      SessionExecution.node.replace(execution),
      LocationServiceMap.node.replace(locations),
    ],
  ),
)

const sessionID = Session.ID.make("ses_schedule_test")

const setup = (directory = enabledDirectory) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values({ id: sessionID, project_id: Project.ID.global, slug: "test", directory, title: "test", version: "test" })
      .run()
      .pipe(Effect.orDie)
    wakes.length = 0
    duringWake = Effect.void
    toggle.on = false
    toggle.reads = 0
    slack.replies = 0
    slack.reads = 0
    slack.posts = 0
    slack.exports = 0
    slack.failing = false
    agentRules = []
    return yield* Schedule.Service
  })

const setTime = (iso: string) => TestClock.setTime(Date.parse(iso))
const pending = Database.Service.use(({ db }) =>
  db
    .select({ id: SessionInboxTable.id })
    .from(SessionInboxTable)
    .all()
    .pipe(
      Effect.orDie,
      Effect.map((rows) => rows.map((row) => row.id)),
    ),
)
/** Moves pending prompts into the transcript, as a running Session would. */
const deliver = Effect.gen(function* () {
  const { db } = yield* Database.Service
  yield* SessionInbox.promote(db, yield* Bus.Service, sessionID, "steer")
})
const messageID = (schedule: Schedule.Info, run: number) => SessionMessage.ID.make(`msg_${schedule.id}_${run}`)
const iso = (time: DateTime.Utc | undefined) => time && DateTime.formatIso(time)

describe("Schedule", () => {
  it.effect("fires an in schedule once", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "in", in: "5 minutes" },
        action: { type: "prompt", prompt: "check the build" },
      })
      expect(schedule.tz).toBe("America/New_York")

      yield* TestClock.adjust("4 minutes")
      yield* schedules.tick
      expect(yield* pending).toEqual([])

      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      expect(yield* pending).toEqual([messageID(schedule, 1)])
      expect(wakes).toEqual([sessionID])
      const { db } = yield* Database.Service
      expect(yield* SessionInbox.find(db, messageID(schedule, 1))).toMatchObject({
        type: "user",
        delivery: "steer",
        payload: { text: "check the build" },
      })
      expect(yield* schedules.get(schedule.id)).toMatchObject({
        status: "done",
        runCount: 1,
        lastOutcome: "fired",
        nextDueAt: undefined,
      })

      yield* TestClock.adjust("10 minutes")
      yield* schedules.tick
      expect(yield* pending).toEqual([messageID(schedule, 1)])
    }),
  )

  it.effect("fires an at schedule at the wall-clock time in its zone", () =>
    Effect.gen(function* () {
      yield* setTime("2026-11-01T12:00:00Z")
      const schedules = yield* setup()
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "at", at: "2026-11-02T09:30", tz: "Europe/Paris" },
        action: { type: "prompt", prompt: "standup" },
      })
      expect(iso(schedule.nextDueAt)).toBe("2026-11-02T08:30:00.000Z")
      expect(
        yield* Effect.flip(
          schedules.create({
            sessionID,
            when: { type: "at", at: "2026-11-01T11:00:00Z" },
            action: { type: "prompt", prompt: "too late" },
          }),
        ),
      ).toMatchObject({ _tag: "Schedule.InvalidError" })

      yield* setTime("2026-11-02T08:30:00Z")
      yield* schedules.tick
      expect(yield* pending).toEqual([messageID(schedule, 1)])
      expect((yield* schedules.get(schedule.id)).status).toBe("done")
    }),
  )

  it.effect("stops an every schedule after ends.times", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "every", every: "1 minute" },
        ends: { times: 2 },
        action: { type: "prompt", prompt: "poll" },
      })
      for (const _ of [1, 2, 3]) {
        yield* TestClock.adjust("1 minute")
        yield* schedules.tick
        yield* deliver
      }
      expect(yield* schedules.get(schedule.id)).toMatchObject({ status: "done", runCount: 2, remainingRuns: 0 })
      expect(wakes).toEqual([sessionID, sessionID])
    }),
  )

  it.effect("stops an every schedule at ends.at", () =>
    Effect.gen(function* () {
      yield* setTime("2026-10-26T12:00:00Z")
      const schedules = yield* setup()
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "every", every: "10 minutes" },
        ends: { at: "2026-10-26T12:25:00Z" },
        action: { type: "prompt", prompt: "poll" },
      })
      for (const _ of [1, 2, 3]) {
        yield* TestClock.adjust("10 minutes")
        yield* schedules.tick
        yield* deliver
      }
      expect(yield* schedules.get(schedule.id)).toMatchObject({ status: "done", runCount: 2 })
    }),
  )

  it.effect("keeps cron at the same wall-clock time across a DST change", () =>
    Effect.gen(function* () {
      yield* setTime("2026-10-31T12:00:00Z")
      const schedules = yield* setup()
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "cron", cron: "30 9 * * *", tz: "America/New_York" },
        action: { type: "prompt", prompt: "morning" },
      })
      expect(iso(schedule.nextDueAt)).toBe("2026-10-31T13:30:00.000Z")

      yield* setTime("2026-10-31T13:30:00Z")
      yield* schedules.tick
      expect(iso((yield* schedules.get(schedule.id)).nextDueAt)).toBe("2026-11-01T14:30:00.000Z")
    }),
  )

  it.effect("catches up once after downtime, marked late, and keeps the phase", () =>
    Effect.gen(function* () {
      yield* setTime("2026-10-26T12:00:00Z")
      const schedules = yield* setup()
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "every", every: "10 minutes" },
        action: { type: "prompt", prompt: "poll" },
      })

      yield* setTime("2026-10-26T13:05:00Z")
      yield* schedules.tick
      yield* schedules.tick
      expect(yield* pending).toEqual([messageID(schedule, 1)])
      const { db } = yield* Database.Service
      expect(yield* SessionInbox.find(db, messageID(schedule, 1))).toMatchObject({
        payload: { metadata: { schedule: { id: schedule.id, run: 1, late: true } } },
      })
      expect(yield* schedules.get(schedule.id)).toMatchObject({ runCount: 1, lastOutcome: "late" })
      expect(iso((yield* schedules.get(schedule.id)).nextDueAt)).toBe("2026-10-26T13:10:00.000Z")
    }),
  )

  it.effect("skips a firing while the previous prompt is unconsumed", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "every", every: "1 minute" },
        action: { type: "prompt", prompt: "poll" },
      })
      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      expect(yield* pending).toEqual([messageID(schedule, 1)])
      expect(yield* schedules.get(schedule.id)).toMatchObject({ runCount: 1, lastOutcome: "skipped" })
      // The skip wakes the Session again in case the wake after admission was lost.
      expect(wakes).toEqual([sessionID, sessionID])

      yield* deliver
      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      expect(yield* pending).toEqual([messageID(schedule, 2)])
    }),
  )

  it.effect("does not duplicate a prompt admitted before a crash", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const sessions = yield* Session.Service
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "in", in: "1 minute" },
        action: { type: "prompt", prompt: "once" },
      })
      yield* TestClock.adjust("1 minute")
      // The previous process admitted run 1 and died before advancing the row.
      yield* sessions.prompt({ sessionID, id: messageID(schedule, 1), text: "once", resume: false })

      yield* schedules.tick
      expect(yield* pending).toEqual([messageID(schedule, 1)])
      expect(wakes).toEqual([sessionID])
      expect(yield* schedules.get(schedule.id)).toMatchObject({ status: "done", runCount: 1 })
    }),
  )

  it.effect("removes a Session's schedules with the Session", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      yield* schedules.create({
        sessionID,
        when: { type: "every", every: "1 hour" },
        action: { type: "prompt", prompt: "poll" },
      })
      const { db } = yield* Database.Service
      yield* db.delete(SessionTable).where(eq(SessionTable.id, sessionID)).run().pipe(Effect.orDie)
      expect(yield* schedules.list()).toEqual([])
    }),
  )

  it.effect("does nothing when experimental.schedules is off", () =>
    Effect.gen(function* () {
      const schedules = yield* setup(disabledDirectory)
      expect(
        yield* Effect.flip(
          schedules.create({
            sessionID,
            when: { type: "in", in: "1 minute" },
            action: { type: "prompt", prompt: "poll" },
          }),
        ),
      ).toMatchObject({ _tag: "Schedule.DisabledError" })

      const { db } = yield* Database.Service
      yield* db
        .insert(ScheduleTable)
        .values({
          id: Schedule.ID.create(),
          session_id: sessionID,
          action: { type: "prompt", prompt: "poll" },
          when: { type: "in", in: "1 minute" },
          tz: "UTC",
          next_due_at: 0,
          time_created: 0,
          time_updated: 0,
        })
        .run()
        .pipe(Effect.orDie)
      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      expect(yield* pending).toEqual([])
      expect((yield* schedules.list())[0]).toMatchObject({ runCount: 0 })
    }),
  )

  it.effect("rechecks a schedule hourly while its location has the flag off, then catches up once it is on", () =>
    Effect.gen(function* () {
      const schedules = yield* setup(toggleDirectory)
      const { db } = yield* Database.Service
      const id = Schedule.ID.create()
      yield* db
        .insert(ScheduleTable)
        .values({
          id,
          session_id: sessionID,
          action: { type: "prompt", prompt: "poll" },
          when: { type: "every", every: "1 minute" },
          tz: "UTC",
          next_due_at: 60_000,
          time_created: 0,
          time_updated: 0,
        })
        .run()
        .pipe(Effect.orDie)
      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      expect(yield* schedules.get(id)).toMatchObject({
        status: "active",
        runCount: 0,
        lastOutcome: "disabled",
        nextDueAt: DateTime.makeUnsafe(60_000),
      })
      const reads = toggle.reads
      expect(reads).toBeGreaterThan(0)

      // Within the hour the location is not booted to read the flag again.
      yield* TestClock.adjust("59 minutes")
      yield* schedules.tick
      expect(toggle.reads).toBe(reads)
      expect(yield* pending).toEqual([])

      // Once the flag is on and the hour has passed, the missed slot fires once, late, and keeps the phase.
      toggle.on = true
      yield* TestClock.adjust("2 minutes")
      yield* schedules.tick
      expect(yield* pending).toEqual([SessionMessage.ID.make(`msg_${id}_1`)])
      expect(yield* schedules.get(id)).toMatchObject({
        status: "active",
        runCount: 1,
        lastOutcome: "late",
        nextDueAt: DateTime.makeUnsafe(63 * 60_000),
      })
    }),
  )

  it.effect("pauses and resumes at the next slot", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "every", every: "1 minute" },
        action: { type: "prompt", prompt: "poll" },
      })
      expect((yield* schedules.pause(schedule.id)).status).toBe("paused")
      yield* TestClock.adjust("150 seconds")
      yield* schedules.tick
      expect(yield* pending).toEqual([])

      const resumed = yield* schedules.resume(schedule.id)
      expect(resumed.status).toBe("active")
      expect(iso(resumed.nextDueAt)).toBe(new Date(180_000).toISOString())
      yield* schedules.remove(schedule.id)
      expect(yield* Effect.flip(schedules.get(schedule.id))).toMatchObject({ _tag: "Schedule.NotFoundError" })
    }),
  )

  it.effect("keeps the run count when paused while a prompt is being admitted", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "every", every: "1 minute" },
        action: { type: "prompt", prompt: "poll" },
      })
      yield* TestClock.adjust("1 minute")
      duringWake = schedules.pause(schedule.id).pipe(Effect.orDie, Effect.asVoid)
      yield* schedules.tick
      expect(yield* schedules.get(schedule.id)).toMatchObject({ status: "paused", runCount: 1 })
      yield* deliver

      yield* schedules.resume(schedule.id)
      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      expect(yield* pending).toEqual([messageID(schedule, 2)])
    }),
  )

  it.effect("run sleeps until the next schedule is due", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const fiber = yield* Effect.forkChild(schedules.run)
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "in", in: "10 seconds" },
        action: { type: "prompt", prompt: "soon" },
      })
      yield* TestClock.adjust("9 seconds")
      expect(yield* pending).toEqual([])
      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow
      expect(yield* pending).toEqual([messageID(schedule, 1)])
      yield* Fiber.interrupt(fiber)
    }),
  )

  it.effect("fires the last slot when the tick lands just after ends_at", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "every", every: "10 minutes" },
        ends: { after: "20 minutes" },
        action: { type: "prompt", prompt: "poll" },
      })
      yield* TestClock.adjust("10 minutes")
      yield* schedules.tick
      yield* deliver
      yield* TestClock.adjust(600_001)
      yield* schedules.tick
      expect(yield* pending).toEqual([messageID(schedule, 2)])
      expect(yield* schedules.get(schedule.id)).toMatchObject({ status: "done", runCount: 2, lastOutcome: "fired" })
    }),
  )

  // On 2026-11-01 New York falls back at 06:00Z, so 1:00-1:59 runs twice: 05:00Z EDT, then 06:00Z EST.
  const cron = (schedules: Schedule.Interface, expression: string) =>
    schedules.create({
      sessionID,
      when: { type: "cron", cron: expression, tz: "America/New_York" },
      action: { type: "prompt", prompt: expression },
    })

  it.effect("does not fire a step cron again in the repeated hour", () =>
    Effect.gen(function* () {
      yield* setTime("2026-11-01T05:50:00Z")
      const schedules = yield* setup()
      const schedule = yield* cron(schedules, "*/10 * * * *")
      // 1:50 EDT fired the last 1:xx slot; the next is 2:00 EST, an hour and ten minutes later.
      expect(iso(schedule.nextDueAt)).toBe("2026-11-01T07:00:00.000Z")

      // Created inside the repeated hour, the next slot is the first one after it, not three hours on.
      yield* setTime("2026-11-01T06:35:00Z")
      expect(iso((yield* cron(schedules, "*/10 * * * *")).nextDueAt)).toBe("2026-11-01T07:00:00.000Z")
      expect(iso((yield* cron(schedules, "0,30 * * * *")).nextDueAt)).toBe("2026-11-01T07:00:00.000Z")
      expect(iso((yield* cron(schedules, "0 * * * *")).nextDueAt)).toBe("2026-11-01T07:00:00.000Z")
    }),
  )

  it.effect("fires a fixed-time cron once in the repeated hour", () =>
    Effect.gen(function* () {
      yield* setTime("2026-11-01T05:00:00Z")
      const schedules = yield* setup()
      const schedule = yield* cron(schedules, "30 1 * * *")
      expect(iso(schedule.nextDueAt)).toBe("2026-11-01T05:30:00.000Z")

      yield* setTime("2026-11-01T05:30:00Z")
      yield* schedules.tick
      expect(iso((yield* schedules.get(schedule.id)).nextDueAt)).toBe("2026-11-02T06:30:00.000Z")

      yield* setTime("2026-11-01T06:05:00Z")
      expect(iso((yield* cron(schedules, "30 1 * * *")).nextDueAt)).toBe("2026-11-02T06:30:00.000Z")
    }),
  )

  it.effect("moves cron past the skipped hour when clocks spring forward", () =>
    Effect.gen(function* () {
      // On 2026-03-08 New York jumps from 2:00 EST to 3:00 EDT at 07:00Z.
      yield* setTime("2026-03-08T06:55:00Z")
      const schedules = yield* setup()
      expect(iso((yield* cron(schedules, "*/10 * * * *")).nextDueAt)).toBe("2026-03-08T07:00:00.000Z")
      expect(iso((yield* cron(schedules, "30 2 * * *")).nextDueAt)).toBe("2026-03-08T07:30:00.000Z")
    }),
  )

  it.effect("rejects a cron that never fires", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const error = yield* Effect.flip(
        schedules.create({
          sessionID,
          when: { type: "cron", cron: "0 0 31 2 *" },
          action: { type: "prompt", prompt: "never" },
        }),
      )
      expect(error).toMatchObject({ _tag: "Schedule.InvalidError" })
    }),
  )

  it.effect("wakes a Session on start when its scheduled prompt was never claimed", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      yield* schedules.create({
        sessionID,
        when: { type: "in", in: "1 minute" },
        action: { type: "prompt", prompt: "once" },
      })
      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      // The process died before the Session claimed the prompt, so that wake was lost.
      wakes.length = 0
      const fiber = yield* Effect.forkChild(schedules.run)
      yield* Effect.yieldNow
      expect(wakes).toEqual([sessionID])
      yield* Fiber.interrupt(fiber)
    }),
  )

  it.effect("parses at times with offsets or in the schedule's zone", () =>
    Effect.gen(function* () {
      yield* setTime("2026-11-01T12:00:00Z")
      const schedules = yield* setup()
      const at = (value: string) =>
        schedules
          .create({ sessionID, when: { type: "at", at: value }, action: { type: "prompt", prompt: "at" } })
          .pipe(Effect.map((schedule) => iso(schedule.nextDueAt)))
      expect(yield* at("2026-11-02T09:30:00-0500")).toBe("2026-11-02T14:30:00.000Z")
      expect(yield* at("2026-11-02T09:30:00-05:00")).toBe("2026-11-02T14:30:00.000Z")
      expect(yield* at("2026-11-02T09:30")).toBe("2026-11-02T14:30:00.000Z")
      expect(yield* at("2026-11-02 09:30:00-05:00")).toBe("2026-11-02T14:30:00.000Z")
      expect(yield* at("2026-11-02 14:30:00Z")).toBe("2026-11-02T14:30:00.000Z")
      expect(yield* at("2026-11-02 09:30")).toBe("2026-11-02T14:30:00.000Z")
    }),
  )

  it.effect("run ticks again at once when a schedule fell due while it was firing", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const first = yield* schedules.create({
        sessionID,
        when: { type: "in", in: "10 seconds" },
        action: { type: "prompt", prompt: "first" },
      })
      const second = yield* schedules.create({
        sessionID,
        when: { type: "in", in: "10500 millis" },
        action: { type: "prompt", prompt: "second" },
      })
      // Firing the first schedule takes a second.
      duringWake = TestClock.adjust("1 second")
      const fiber = yield* Effect.forkChild(schedules.run)
      yield* Effect.yieldNow
      yield* TestClock.adjust("10 seconds")
      // Without moving the clock again, the loop must tick a second time on its own.
      const ids = yield* Effect.yieldNow.pipe(
        Effect.andThen(pending),
        Effect.repeat({ until: (ids) => ids.length >= 2, times: 1000 }),
      )
      expect(ids).toEqual([messageID(first, 1), messageID(second, 1)])
      yield* Fiber.interrupt(fiber)
    }),
  )

  it.effect("backs off for five minutes when a prompt fails, then fires once", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const schedule = yield* schedules.create({
        sessionID,
        when: { type: "in", in: "1 minute" },
        action: { type: "prompt", prompt: "flaky" },
      })
      yield* TestClock.adjust("1 minute")
      duringWake = Effect.die("wake failed")
      yield* schedules.tick
      expect(yield* schedules.get(schedule.id)).toMatchObject({ status: "active", runCount: 0, lastOutcome: "failed" })

      // Not retried on every tick.
      wakes.length = 0
      yield* TestClock.adjust("4 minutes")
      yield* schedules.tick
      expect(wakes).toEqual([])

      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      expect(wakes).toEqual([sessionID])
      expect(yield* pending).toEqual([messageID(schedule, 1)])
      expect(yield* schedules.get(schedule.id)).toMatchObject({ status: "done", runCount: 1, lastOutcome: "late" })
    }),
  )

  it.effect("rejects a zero duration or ends.times below 1", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const create = (input: Omit<Schedule.CreateInput, "sessionID" | "action">) =>
        Effect.flip(schedules.create({ sessionID, action: { type: "prompt", prompt: "x" }, ...input }))
      expect(yield* create({ when: { type: "in", in: "0 seconds" } })).toMatchObject({
        _tag: "Schedule.InvalidError",
        reason: "Duration must be positive: 0 seconds",
      })
      expect(yield* create({ when: { type: "every", every: "1 hour" }, ends: { times: 0 } })).toMatchObject({
        _tag: "Schedule.InvalidError",
        reason: "ends.times must be at least 1",
      })
      expect(yield* schedules.list({ sessionID })).toEqual([])
    }),
  )

  describe("code condition", () => {
    /** Creates a check while the user answers each permission it asks for with `reply`; returns what was asked. */
    const answer = (code: string, allow: ReadonlyArray<string>, reply: Permission.Reply) =>
      Effect.gen(function* () {
        const schedules = yield* Schedule.Service
        const bus = yield* Bus.Service
        const instances = yield* Instance.Service
        const sessions = yield* Session.Service
        const session = yield* sessions.get(sessionID)
        const asks = yield* Queue.unbounded<Permission.Request>()
        const unsubscribe = yield* bus.listen((event) =>
          event.type === Permission.Event.Asked.type
            ? Queue.offer(asks, event.data as Permission.Request).pipe(Effect.asVoid)
            : Effect.void,
        )
        yield* Effect.addFinalizer(() => unsubscribe)
        const asked: Permission.Request[] = []
        const replies = yield* Queue.take(asks).pipe(
          Effect.tap((request) => Effect.sync(() => asked.push(request))),
          Effect.flatMap((request) =>
            Permission.Service.use((permission) => permission.reply({ requestID: request.id, reply })).pipe(
              instances.provide(session),
            ),
          ),
          Effect.forever,
          Effect.forkChild,
        )
        const created = yield* schedules
          .create({
            sessionID,
            name: "thread",
            when: { type: "every", every: "5 minutes" },
            if: { type: "code", code, allow },
            action: { type: "prompt", prompt: "The thread changed." },
          })
          .pipe(Effect.exit)
        yield* Fiber.interrupt(replies)
        return { created, asked }
      })
    // Approves once, so every scheduled run would still ask if it were not unattended.
    const create = (code: string, allow = ["slack.read_thread"]) =>
      answer(code, allow, "once").pipe(Effect.flatMap((result) => result.created))
    const state = (schedule: Schedule.Info) =>
      Database.Service.use(({ db }) =>
        db
          .select({ state: ScheduleTable.state })
          .from(ScheduleTable)
          .where(eq(ScheduleTable.id, schedule.id))
          .get()
          .pipe(
            Effect.orDie,
            Effect.map((row) => row?.state),
          ),
      )
    const watch = `
      const thread = await tools.slack.read_thread({})
      return {
        wake: state !== null && thread.replies > state.replies,
        state: { replies: thread.replies, runs },
        message: thread.replies + " replies",
      }
    `

    it.effect("advances and keeps its state without prompting when it does not wake", () =>
      Effect.gen(function* () {
        const schedules = yield* setup()
        const schedule = yield* create(watch)
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        expect(yield* pending).toEqual([])
        expect(wakes).toEqual([])
        // The read would ask with no rules; an allowlisted tool runs without asking.
        expect(slack.reads).toBe(1)
        expect(yield* state(schedule)).toEqual({ replies: 0, runs: 0 })
        const info = yield* schedules.get(schedule.id)
        expect(info).toMatchObject({ status: "active", runCount: 1, lastOutcome: "checked" })
        expect(iso(info.nextDueAt)).toBe(iso(DateTime.makeUnsafe(Date.parse("1970-01-01T00:10:00Z"))))
      }),
    )

    it.effect("prompts once with its message when it wakes, carrying state between runs", () =>
      Effect.gen(function* () {
        const schedules = yield* setup()
        const schedule = yield* create(watch)
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        slack.replies = 2
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        expect(yield* pending).toEqual([messageID(schedule, 2)])
        const { db } = yield* Database.Service
        expect(yield* SessionInbox.find(db, messageID(schedule, 2))).toMatchObject({
          payload: {
            text: "The thread changed.\n\n2 replies",
            metadata: { schedule: { id: schedule.id, run: 2, check: { message: "2 replies" } } },
          },
        })
        expect(yield* schedules.get(schedule.id)).toMatchObject({ runCount: 2, lastOutcome: "fired" })
        expect(yield* state(schedule)).toEqual({ replies: 2, runs: 1 })

        yield* deliver
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        expect(yield* pending).toEqual([])
        expect(yield* schedules.get(schedule.id)).toMatchObject({ runCount: 3, lastOutcome: "checked" })
        expect(yield* state(schedule)).toEqual({ replies: 2, runs: 2 })
      }),
    )

    it.effect("cannot call a tool outside its allowlist", () =>
      Effect.gen(function* () {
        const schedules = yield* setup()
        const schedule = yield* create(`await tools.slack.post({}); return { wake: true, state: null }`)
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        expect(slack.posts).toBe(0)
        expect(yield* pending).toEqual([])
        expect(yield* schedules.get(schedule.id)).toMatchObject({ runCount: 0, lastOutcome: "failed" })
      }),
    )

    it.effect("asks the user to approve each allowlisted tool when it is created", () =>
      Effect.gen(function* () {
        const schedules = yield* setup()
        agentRules = [{ action: "slack_read_thread", resource: "*", effect: "ask" }]
        const approved = yield* answer(watch, ["slack.read_thread"], "once")
        expect(approved.asked).toMatchObject([
          { action: "slack_read_thread", metadata: { code: watch, allow: ["slack.read_thread"] } },
        ])
        expect(approved.asked[0]?.save).toBeUndefined()
        expect(Exit.isSuccess(approved.created)).toBe(true)

        const rejected = yield* answer(watch, ["slack.read_thread"], "reject")
        expect(rejected.asked).toHaveLength(1)
        expect(Exit.isFailure(rejected.created)).toBe(true)
        expect(yield* schedules.list({ sessionID })).toHaveLength(1)
      }),
    )

    it.effect("is not created when the agent's rules deny an allowlisted tool, or the tool is unknown", () =>
      Effect.gen(function* () {
        const schedules = yield* setup()
        agentRules = [{ action: "slack_read_thread", resource: "*", effect: "deny" }]
        const denied = yield* answer(watch, ["slack.read_thread"], "once")
        expect(denied.asked).toEqual([])
        expect(denied.created).toMatchObject({
          _tag: "Failure",
          cause: { reasons: [{ error: { _tag: "Permission.BlockedError" } }] },
        })
        expect(yield* Effect.flip(create(watch, ["slack.missing"]))).toMatchObject({
          _tag: "Schedule.InvalidError",
          reason: "Unknown Code Mode tool: slack.missing",
        })
        expect(yield* schedules.list({ sessionID })).toEqual([])
      }),
    )

    it.effect("denies an ask for a permission other than an allowlisted tool's own", () =>
      Effect.gen(function* () {
        const schedules = yield* setup()
        const schedule = yield* create(`await tools.slack.export_thread({}); return { wake: true, state: null }`, [
          "slack.export_thread",
        ])
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        expect(slack.exports).toBe(0)
        expect(yield* pending).toEqual([])
        expect(yield* schedules.get(schedule.id)).toMatchObject({ runCount: 0, lastOutcome: "failed" })
      }),
    )

    it.effect("keeps its state and run count when a run fails", () =>
      Effect.gen(function* () {
        const schedules = yield* setup()
        const schedule = yield* create(watch)
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        slack.failing = true
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        expect(yield* schedules.get(schedule.id)).toMatchObject({ runCount: 1, lastOutcome: "failed" })
        expect(yield* state(schedule)).toEqual({ replies: 0, runs: 0 })
      }),
    )

    it.effect("does not run while its last wake is unread", () =>
      Effect.gen(function* () {
        const schedules = yield* setup()
        const schedule = yield* create(`await tools.slack.read_thread({}); return { wake: true, state: runs }`)
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        expect(yield* pending).toEqual([messageID(schedule, 1)])
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        expect(slack.reads).toBe(1)
        expect(yield* pending).toEqual([messageID(schedule, 1)])
        expect(yield* schedules.get(schedule.id)).toMatchObject({ runCount: 1, lastOutcome: "skipped" })
        expect(yield* state(schedule)).toBe(0)

        yield* deliver
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        expect(yield* pending).toEqual([messageID(schedule, 2)])
        expect(yield* state(schedule)).toBe(1)
      }),
    )

    it.effect("may declare its own state, and wakes with just the prompt when its message is empty", () =>
      Effect.gen(function* () {
        const schedules = yield* setup()
        const schedule = yield* create(`const state = 7\nreturn { wake: true, state, message: "" }`, [])
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        const { db } = yield* Database.Service
        expect(yield* SessionInbox.find(db, messageID(schedule, 1))).toMatchObject({
          payload: { text: "The thread changed." },
        })
        expect(yield* state(schedule)).toBe(7)
      }),
    )

    it.effect("backs off like a failed prompt when the program fails or returns no wake", () =>
      Effect.gen(function* () {
        const schedules = yield* setup()
        const failing = yield* create(`await tools.slack.read_thread({}); throw new Error("boom")`)
        const invalid = yield* create(`return { state: null }`)
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        expect(slack.reads).toBe(1)
        expect(yield* schedules.get(failing.id)).toMatchObject({ runCount: 0, lastOutcome: "failed" })
        expect(yield* schedules.get(invalid.id)).toMatchObject({ runCount: 0, lastOutcome: "failed" })

        yield* TestClock.adjust("4 minutes")
        yield* schedules.tick
        expect(slack.reads).toBe(1)
        yield* TestClock.adjust("1 minute")
        yield* schedules.tick
        expect(slack.reads).toBe(2)
        expect(yield* pending).toEqual([])
      }),
    )
  })
})
