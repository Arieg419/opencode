export * as SubagentJob from "./subagent-job.js"

import { and, gte, lt, sql } from "drizzle-orm"
import { Context, Effect, Layer, Option, Schema, Scope, Stream } from "effect"
import { Node } from "@opencode/util/effect/app-node"
import type { LayerNode } from "@opencode/util/effect/layer-node"
import { Bus } from "../bus.js"
import { Database } from "../database/database.js"
import { KeyedMutex } from "../effect/keyed-mutex.js"
import { Job } from "../job.js"
import { KV } from "../kv.js"
import { KVTable } from "../kv/sql.js"
import { Session } from "../session.js"
import { SessionEvent } from "./event.js"
import { SessionMessage } from "./message.js"
import { SessionSchema } from "./schema.js"
import { SessionMessageTable, SessionTable } from "./sql.js"
import { SessionStore } from "./store.js"
import { SubagentCompletion } from "./subagent-completion.js"

type Recovery = Extract<Job.Recovery, { kind: "subagent" }>

/**
 * Durable parent-reporting state for a subagent child. `reported` is the idle message ID of
 * the newest busy period already handled, so later turns and server restarts continue from it.
 */
const Marker = Schema.Struct({
  parentSessionID: SessionSchema.ID,
  agent: Schema.String,
  description: Schema.String,
  reported: Schema.optionalKey(SessionMessage.ID),
})
type Marker = typeof Marker.Type

const decodeMarker = Schema.decodeUnknownOption(Marker)
const markerPrefix = "subagent.parent/"
const markerPrefixEnd = "subagent.parent0"
const childIndexPrefix = "subagent.child/"
const markerKey = (sessionID: SessionSchema.ID) => `${markerPrefix}${sessionID}`
const childIndexKey = (parentSessionID: SessionSchema.ID, childSessionID: SessionSchema.ID) =>
  `${childIndexPrefix}${parentSessionID}/${childSessionID}`
const noticeID = (idleID: SessionMessage.ID) => SessionMessage.ID.make(`${idleID}_subagent`)
const TURN_PAGE = 50

export interface Interface {
  track: (recovery: Recovery) => Effect.Effect<void>
  ensure: (recovery: Recovery) => Effect.Effect<void>
  start: (recovery: Recovery) => Effect.Effect<Job.Info>
  block: <E>(recovery: Recovery, prompt: Effect.Effect<unknown, E>) => Effect.Effect<Job.BlockResult | undefined, E>
  background: (recovery: Recovery) => Effect.Effect<void>
  notify: (recovery: Recovery, startedAt: number) => Effect.Effect<void>
  recover: <A, E, R>(
    suspended: ReadonlySet<SessionSchema.ID>,
    prepare: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SubagentJob") {}

const make: Effect.Effect<
  Interface,
  never,
  Database.Service | Session.Service | SessionStore.Service | Job.Service | Bus.Service | KV.Service | Scope.Scope
> = Effect.gen(function* () {
  const db = (yield* Database.Service).db
  const sessions = yield* Session.Service
  const store = yield* SessionStore.Service
  const jobs = yield* Job.Service
  const bus = yield* Bus.Service
  const kv = yield* KV.Service
  const scope = yield* Scope.Scope
  // Jobs and listeners outlive the plugin that launched them, without retaining a caller Location.
  const context = yield* Effect.context<never>()
  const locks = KeyedMutex.makeUnsafe<SessionSchema.ID>()
  const notifications = new Set<string>()
  // Holds idle delivery while a synchronous foreground call is waiting on `jobs.block`.
  const foreground = new Map<SessionSchema.ID, number>()
  const settled = new Map<SessionSchema.ID, SessionMessage.ID>()
  const deferredParents = new Set<SessionSchema.ID>()

  const readMarker = (sessionID: SessionSchema.ID) =>
    kv.get(markerKey(sessionID)).pipe(Effect.map((value) => Option.getOrUndefined(decodeMarker(value))))

  const writeMarker = (sessionID: SessionSchema.ID, marker: Marker) =>
    Effect.all(
      [
        kv.set(markerKey(sessionID), {
          parentSessionID: marker.parentSessionID,
          agent: marker.agent,
          description: marker.description,
          ...(marker.reported ? { reported: marker.reported } : {}),
        }),
        kv.set(childIndexKey(marker.parentSessionID, sessionID), ""),
      ],
      { discard: true },
    )

  const removeMarker = (childSessionID: SessionSchema.ID, parentSessionID?: SessionSchema.ID) =>
    Effect.all(
      [
        kv.remove(markerKey(childSessionID)),
        parentSessionID ? kv.remove(childIndexKey(parentSessionID, childSessionID)) : Effect.void,
      ],
      { discard: true },
    )

  const latestIdle = (sessionID: SessionSchema.ID) =>
    store.messages({ sessionID, type: "idle", order: "desc", limit: 1 }).pipe(
      Effect.map(([message]) => (message?.type === "idle" ? message.id : undefined)),
      Effect.orDie,
    )

  // Startup only visits markers that are orphaned or behind the child's newest projected idle row.
  const dirtyMarkers = Effect.gen(function* () {
    const start = markerPrefix.length + 1
    const rows = yield* db
      .select({ key: KVTable.key })
      .from(KVTable)
      .where(
        and(
          gte(KVTable.key, markerPrefix),
          lt(KVTable.key, markerPrefixEnd),
          sql`(
            not exists (
              select 1 from ${SessionTable} child
              join ${SessionTable} parent on parent.id = json_extract(${KVTable.value}, '$.parentSessionID')
              where child.id = substr(${KVTable.key}, ${start})
                and child.parent_id = parent.id
            )
            or coalesce(json_extract(${KVTable.value}, '$.reported'), '') != coalesce((
              select m.id from ${SessionMessageTable} m
              where m.session_id = substr(${KVTable.key}, ${start})
                and m.type = 'idle'
              order by m.seq desc
              limit 1
            ), '')
          )`,
        ),
      )
      .all()
      .pipe(Effect.orDie)
    return rows.flatMap((row) => {
      const id = Schema.decodeUnknownOption(SessionSchema.ID)(row.key.slice(markerPrefix.length))
      return Option.isSome(id) ? [id.value] : []
    })
  })

  // A revert can delete the previously reported idle boundary; re-anchor to the newest pre-revert idle.
  const reanchor = Effect.fnUntraced(function* (sessionID: SessionSchema.ID, marker: Marker) {
    if (!marker.reported || (yield* store.message(marker.reported))) return marker.reported
    const idles = yield* store.messages({ sessionID, type: "idle", order: "desc", limit: TURN_PAGE }).pipe(Effect.orDie)
    const reported = idles.find(
      (idle): idle is SessionMessage.Idle => idle.type === "idle" && idle.id < marker.reported!,
    )?.id
    yield* writeMarker(sessionID, { ...marker, reported })
    return reported
  })

  const deliverToParent = Effect.fnUntraced(function* (
    childSessionID: SessionSchema.ID,
    marker: Marker,
    idleID: SessionMessage.ID,
    outcome: { status: "completed"; output: string } | { status: "error"; error: string },
  ) {
    return yield* SubagentCompletion.deliver(sessions, jobs, {
      inboxID: noticeID(idleID),
      recovery: {
        kind: "subagent",
        parentSessionID: marker.parentSessionID,
        childSessionID,
        agent: marker.agent,
        description: marker.description,
      },
      ...outcome,
      resume: deferredParents.has(marker.parentSessionID) ? false : undefined,
    }).pipe(
      Effect.as(true),
      Effect.catchTag("Session.NotFoundError", () =>
        removeMarker(childSessionID, marker.parentSessionID).pipe(Effect.as(false)),
      ),
      Effect.orDie,
    )
  })

  // Every unreported busy period of a child, read forward in sequence order from its saved marker.
  // Text results and failures notify the parent; empty successes, interruptions, and busy periods
  // started by a direct user prompt advance the marker silently.
  const syncUnlocked = Effect.fnUntraced(function* (sessionID: SessionSchema.ID) {
    if ((foreground.get(sessionID) ?? 0) > 0) return
    const marker = yield* readMarker(sessionID)
    if (!marker) return
    const child = yield* store.get(sessionID)
    if (!child || child.parentID !== marker.parentSessionID || !(yield* store.get(marker.parentSessionID))) {
      yield* removeMarker(sessionID, marker.parentSessionID)
      return
    }

    let reported = marker.reported
    let cursor = reported ? { id: reported, direction: "next" as const } : undefined
    let assistant: SessionMessage.Assistant | undefined
    let starter: "subagent" | "direct" | undefined
    let checkedAnchor = false
    while (true) {
      const page = yield* store.messages({ sessionID, order: "asc", limit: TURN_PAGE, cursor }).pipe(Effect.orDie)
      if (page.length === 0 && reported && !checkedAnchor) {
        checkedAnchor = true
        const anchored = yield* reanchor(sessionID, marker)
        if (anchored !== reported) {
          reported = anchored
          cursor = reported ? { id: reported, direction: "next" } : undefined
          continue
        }
      }
      for (const message of page) {
        if (starter === undefined && (message.type === "user" || message.type === "synthetic"))
          starter = message.type === "user" && message.metadata?.source !== "subagent" ? "direct" : "subagent"
        if (message.type === "assistant" && message.time.completed !== undefined && message.error === undefined)
          assistant = message
        if (message.type !== "idle") continue

        const shouldDeliver = starter !== "direct" && message.outcome !== "interrupted"
        if (shouldDeliver && message.outcome === "failed") {
          const delivered = yield* deliverToParent(sessionID, marker, message.id, {
            status: "error",
            error: message.error?.message ?? "Subagent failed",
          })
          if (!delivered) return
        }
        if (shouldDeliver && message.outcome === "succeeded") {
          const output = SubagentCompletion.extractText(assistant)
          if (output !== undefined) {
            const delivered = yield* deliverToParent(sessionID, marker, message.id, {
              status: "completed",
              output,
            })
            if (!delivered) return
          }
        }
        reported = message.id
        yield* writeMarker(sessionID, { ...marker, reported })
        assistant = undefined
        starter = undefined
      }
      const last = page.at(-1)
      if (page.length < TURN_PAGE || !last) return
      cursor = { id: last.id, direction: "next" }
    }
  })

  const sync = (sessionID: SessionSchema.ID) => locks.withLock(sessionID)(syncUnlocked(sessionID))

  const clearChild = (childSessionID: SessionSchema.ID, parentSessionID?: SessionSchema.ID) =>
    locks.withLock(childSessionID)(
      Effect.gen(function* () {
        const existing = parentSessionID ? undefined : yield* readMarker(childSessionID)
        foreground.delete(childSessionID)
        settled.delete(childSessionID)
        yield* removeMarker(childSessionID, parentSessionID ?? existing?.parentSessionID)
      }),
    )

  const forget = Effect.fnUntraced(function* (sessionID: SessionSchema.ID) {
    yield* clearChild(sessionID)
    const prefix = `${childIndexPrefix}${sessionID}/`
    let after: string | undefined
    do {
      const page = yield* kv.scan({ prefix, after })
      yield* Effect.forEach(
        page.entries,
        (entry) => {
          const id = Schema.decodeUnknownOption(SessionSchema.ID)(entry.key.slice(prefix.length))
          return Option.isSome(id) && id.value !== sessionID ? clearChild(id.value, sessionID) : kv.remove(entry.key)
        },
        { discard: true },
      )
      after = page.next
    } while (after)
  })

  const latestTurnOutput = Effect.fnUntraced(function* (sessionID: SessionSchema.ID) {
    let seenIdle = false
    let assistant: SessionMessage.Assistant | undefined
    let cursor: { id: SessionMessage.ID; direction: "next" } | undefined
    while (true) {
      const page = yield* store.messages({ sessionID, order: "desc", limit: TURN_PAGE, cursor }).pipe(Effect.orDie)
      for (const message of page) {
        if (message.type === "idle") {
          if (seenIdle) return SubagentCompletion.text(assistant)
          seenIdle = true
          settled.set(sessionID, message.id)
          continue
        }
        if (
          !assistant &&
          message.type === "assistant" &&
          message.time.completed !== undefined &&
          message.error === undefined
        )
          assistant = message
      }
      const last = page.at(-1)
      if (page.length < TURN_PAGE || !last) return SubagentCompletion.text(assistant)
      cursor = { id: last.id, direction: "next" }
    }
  })

  yield* bus
    .subscribe([
      SessionEvent.Execution.Succeeded,
      SessionEvent.Execution.Failed,
      SessionEvent.Execution.Interrupted,
      SessionEvent.RevertEvent.Committed,
      SessionEvent.Deleted,
    ])
    .pipe(
      Stream.runForEach((event) => {
        const sessionID = event.data.sessionID
        if (event.type === SessionEvent.Deleted.type)
          return forget(sessionID).pipe(
            Effect.catchCause((cause) =>
              Effect.logError("subagent.forget_failed", cause).pipe(Effect.annotateLogs({ sessionID })),
            ),
          )
        if (event.type === SessionEvent.Execution.Interrupted.type && event.data.reason === "shutdown")
          return Effect.void
        if (event.type === SessionEvent.RevertEvent.Committed.type)
          return locks
            .withLock(sessionID)(
              readMarker(sessionID).pipe(
                Effect.flatMap((marker) => (marker ? reanchor(sessionID, marker).pipe(Effect.asVoid) : Effect.void)),
              ),
            )
            .pipe(
              Effect.catchCause((cause) =>
                Effect.logError("subagent.reanchor_failed", cause).pipe(Effect.annotateLogs({ sessionID })),
              ),
            )
        return sync(sessionID).pipe(
          Effect.catchCause((cause) =>
            Effect.logError("subagent.sync_failed", cause).pipe(Effect.annotateLogs({ sessionID })),
          ),
        )
      }),
      Effect.forkIn(scope, { startImmediately: true }),
      Effect.setContext(context),
    )

  const notify = Effect.fn("SubagentJob.notify")(function* (recovery: Recovery, startedAt: number) {
    const key = `${recovery.childSessionID}:${startedAt}`
    if (notifications.has(key)) return
    notifications.add(key)
    yield* Effect.gen(function* () {
      const info = (yield* jobs.wait({ id: recovery.childSessionID })).info
      if (!info || info.status === "running") return
      // Completions and errors go through the idle-marker path; the job only owns cancellation and cleanup.
      if (info.status === "cancelled") {
        yield* sessions.interrupt(recovery.childSessionID)
        yield* SubagentCompletion.deliver(sessions, jobs, { ...info, recovery })
        return
      }
      if (info.notificationID) yield* jobs.completeBackground(info.notificationID)
    }).pipe(
      Effect.ensuring(Effect.sync(() => notifications.delete(key))),
      Effect.forkIn(scope, { startImmediately: true }),
      Effect.setContext(context),
    )
  })

  const start = (recovery: Recovery) =>
    jobs
      .start({
        id: recovery.childSessionID,
        type: "subagent",
        title: recovery.description,
        metadata: {},
        recovery,
        run: Effect.gen(function* () {
          settled.delete(recovery.childSessionID)
          yield* sessions.resume(recovery.childSessionID).pipe(
            Effect.onError(() =>
              latestIdle(recovery.childSessionID).pipe(
                Effect.map((idle) => {
                  if (idle) settled.set(recovery.childSessionID, idle)
                }),
                Effect.ignore,
              ),
            ),
          )
          return yield* latestTurnOutput(recovery.childSessionID)
        }),
      })
      .pipe(Effect.setContext(context))

  const track = Effect.fn("SubagentJob.track")((recovery: Recovery) =>
    locks.withLock(recovery.childSessionID)(
      Effect.gen(function* () {
        const existing = yield* readMarker(recovery.childSessionID)
        if (existing) yield* syncUnlocked(recovery.childSessionID)
        const refreshed = existing ? yield* readMarker(recovery.childSessionID) : undefined
        const reported = refreshed ? refreshed.reported : yield* latestIdle(recovery.childSessionID)
        yield* writeMarker(recovery.childSessionID, {
          parentSessionID: recovery.parentSessionID,
          agent: recovery.agent,
          description: recovery.description,
          reported,
        })
      }),
    ),
  )

  const ensure = Effect.fn("SubagentJob.ensure")((recovery: Recovery) =>
    locks.withLock(recovery.childSessionID)(
      Effect.gen(function* () {
        if (yield* readMarker(recovery.childSessionID)) return
        yield* writeMarker(recovery.childSessionID, {
          parentSessionID: recovery.parentSessionID,
          agent: recovery.agent,
          description: recovery.description,
          reported: yield* latestIdle(recovery.childSessionID),
        })
      }),
    ),
  )

  const block: Interface["block"] = (recovery, prompt) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const childID = recovery.childSessionID
        foreground.set(childID, (foreground.get(childID) ?? 0) + 1)
        const releaseForeground = (consumeSettled: boolean) =>
          locks.withLock(childID)(
            Effect.gen(function* () {
              if (consumeSettled) {
                const marker = yield* readMarker(childID)
                const idle = settled.get(childID)
                if (marker && idle && marker.reported !== idle)
                  yield* writeMarker(childID, { ...marker, reported: idle })
              }
              const remaining = (foreground.get(childID) ?? 1) - 1
              if (remaining <= 0) {
                foreground.delete(childID)
                settled.delete(childID)
                yield* syncUnlocked(childID)
                return
              }
              foreground.set(childID, remaining)
            }),
          )

        const prompted = yield* restore(prompt).pipe(Effect.exit)
        if (prompted._tag === "Failure") {
          yield* releaseForeground(false).pipe(Effect.ignore)
          return yield* Effect.failCause(prompted.cause)
        }
        yield* start(recovery)
        const outcome = yield* restore(
          jobs.block({ id: childID, sessionID: recovery.parentSessionID }).pipe(
            Effect.onInterrupt(() =>
              Effect.all([sessions.interrupt(childID), jobs.cancel(childID)], {
                discard: true,
              }),
            ),
          ),
        ).pipe(Effect.exit)
        if (outcome._tag === "Failure") {
          yield* releaseForeground(false).pipe(Effect.ignore)
          return yield* Effect.failCause(outcome.cause)
        }
        if (outcome.value?.type === "backgrounded") {
          yield* notify(recovery, outcome.value.info.started_at)
          yield* releaseForeground(false)
          return outcome.value
        }
        if (outcome.value?.info.status === "cancelled") yield* sessions.interrupt(childID)
        yield* releaseForeground(true)
        return outcome.value
      }),
    )

  const recover: Interface["recover"] = (suspended, prepare) =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        for (const id of suspended) deferredParents.add(id)
      }),
      () =>
        prepare.pipe(
          Effect.tap(() =>
            dirtyMarkers.pipe(
              Effect.flatMap((ids) => Effect.forEach(ids, (childID) => sync(childID), { discard: true })),
            ),
          ),
        ),
      () =>
        Effect.sync(() => {
          for (const id of suspended) deferredParents.delete(id)
        }),
    )

  return Service.of({
    track,
    ensure,
    start,
    block,
    background: Effect.fn("SubagentJob.background")(function* (recovery: Recovery) {
      const info = yield* jobs.background(recovery.childSessionID)
      if (info) yield* notify(recovery, info.started_at)
    }),
    notify,
    recover,
  })
})

export const layer = Layer.effect(Service, make)

export const node: LayerNode.Provider<Service, never, typeof Node.tags.values.global> = Node.makeGlobalNode({
  service: Service,
  layer,
  deps: [Database.node, Session.node, SessionStore.node, Job.node, Bus.node, KV.node],
})
