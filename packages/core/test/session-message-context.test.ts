import { describe, expect, test } from "bun:test"
import { Message } from "@opencode/ai/effect"
import { Bus } from "@opencode/core/bus"
import { Config } from "@opencode/core/config"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath } from "@opencode/core/schema"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionHistory } from "@opencode/core/session/history"
import { SessionInbox } from "@opencode/core/session/inbox"
import { SessionMessage } from "@opencode/core/session/message"
import { MessageContext } from "@opencode/core/session/message-context"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionRecentText } from "@opencode/core/session/recent-text"
import { toLLMMessages } from "@opencode/core/session/runner/to-llm-message"
import { SessionSchema } from "@opencode/core/session/schema"
import { SessionTable } from "@opencode/core/session/sql"
import { SessionStore } from "@opencode/core/session/store"
import { Document, Info } from "@opencode/schema/config"
import { ConfigExperimental } from "@opencode/schema/config/experimental"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { eq } from "drizzle-orm"
import { DateTime, Effect } from "effect"
import { testEffect } from "./lib/effect"

const created = DateTime.makeUnsafe(0)
const model = Model.Ref.make({ id: Model.ID.make("model"), providerID: Provider.ID.make("provider") })
const senderID = SessionSchema.ID.make("ses_context_sender")
const receiverID = SessionSchema.ID.make("ses_context_receiver")
const ORIGIN = MessageContext.ORIGIN

const user = (text: string, metadata?: Record<string, unknown>) =>
  SessionMessage.User.make({
    id: SessionMessage.ID.make("msg_context_user"),
    type: "user",
    text,
    metadata,
    time: { created },
  })

describe("MessageContext.format", () => {
  test("renders a session origin with its title, or unknown without one", () => {
    expect(MessageContext.format({ [ORIGIN]: { kind: "session", sessionID: senderID, title: "OPENCODE IOS" } })).toBe(
      `<message-origin session="${senderID}" title="OPENCODE IOS"/>`,
    )
    expect(MessageContext.format({ [ORIGIN]: { kind: "session", sessionID: senderID } })).toBe(
      `<message-origin session="${senderID}" title="unknown"/>`,
    )
  })

  test("renders a tool origin by name", () => {
    expect(MessageContext.format({ [ORIGIN]: { kind: "tool", name: "ios-train" } })).toBe(
      `<message-origin tool="ios-train"/>`,
    )
  })

  test("escapes attribute values", () => {
    expect(
      MessageContext.format({ [ORIGIN]: { kind: "session", sessionID: senderID, title: `Fix "<b>" & more` } }),
    ).toBe(`<message-origin session="${senderID}" title="Fix &quot;&lt;b&gt;&quot; &amp; more"/>`)
  })

  test("renders nothing for metadata without context or a value that doesn't decode", () => {
    expect(MessageContext.format(undefined)).toBe("")
    expect(MessageContext.format({ source: "api" })).toBe("")
    for (const value of [
      "ses_context_sender",
      { kind: "session" },
      { kind: "session", sessionID: "not-a-session" },
      { kind: "tool" },
      { kind: "agent", name: "build" },
      null,
    ])
      expect(MessageContext.format({ [ORIGIN]: value })).toBe("")
  })
})

describe("prompt builders", () => {
  const origin = { [ORIGIN]: { kind: "session", sessionID: senderID, title: "OPENCODE IOS" } }
  const line = `<message-origin session="${senderID}" title="OPENCODE IOS"/>`

  test("toLLMMessages puts the context line before the text the person typed", () => {
    expect(toLLMMessages([user("rebase the PRs", origin)], model)[0]?.content).toEqual([
      Message.text(`${line}\nrebase the PRs`),
    ])
    expect(toLLMMessages([user("", origin)], model)[0]?.content).toEqual([Message.text(line)])
  })

  test("toLLMMessages output is unchanged without context", () => {
    expect(toLLMMessages([user("rebase the PRs", { source: "api" })], model)).toEqual([
      Message.make({
        id: SessionMessage.ID.make("msg_context_user"),
        role: "user",
        content: [Message.text("rebase the PRs")],
        metadata: { source: "api" },
      }),
    ])
  })

  test("compaction's recent text gives the message one [User]: line", () => {
    const text = SessionRecentText.messageToText(user("rebase the PRs", origin))
    expect(text).toBe(`[User]: ${line}\nrebase the PRs`)
    expect(text.match(/\[User\]:/g)).toHaveLength(1)
  })
})

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Database.node, Bus.node, SessionProjector.node, SessionInbox.node, SessionStore.node]),
    [Bus.node.replace(Bus.configured({ persist: true }))],
  ),
)

const flag = (message_context: boolean) =>
  Config.testLayer([
    new Document({
      type: "document",
      info: new Info({ experimental: new ConfigExperimental.Info({ message_context }) }),
    }),
  ])

const setup = (title: string | undefined) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const bus = yield* Bus.Service
    const inbox = yield* SessionInbox.Service
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .run()
    for (const [sessionID, slug] of [
      [senderID, "sender"],
      [receiverID, "receiver"],
    ] as const)
      yield* bus.publish(SessionEvent.Created, {
        sessionID,
        projectID: Project.ID.global,
        location: { directory: AbsolutePath.make("/project") },
        slug,
        ...(sessionID === senderID && title !== undefined ? { title } : {}),
        version: "test",
      })
    const prompt = Effect.fnUntraced(function* (text: string, metadata?: Record<string, unknown>) {
      const id = SessionMessage.ID.create()
      yield* inbox.admit({
        id,
        sessionID: receiverID,
        item: { type: "user", payload: { text, metadata }, delivery: "steer" },
      })
      yield* bus.publish(SessionEvent.InboxDelivered, { sessionID: receiverID, inboxID: id })
      return id
    })
    const texts = (enabled: boolean) =>
      SessionHistory.load(db, receiverID, "local").pipe(
        Effect.provide(flag(enabled)),
        Effect.map((history) =>
          toLLMMessages(history, model).flatMap((message) =>
            message.content.flatMap((part) => (part.type === "text" ? [part.text] : [])),
          ),
        ),
      )
    return { db, bus, prompt, texts }
  })

const tag = (title: string) => `<message-origin session="${senderID}" title="${title}"/>`

describe("SessionHistory message context", () => {
  it.effect("with the flag off, drops the key so the request matches a message without it", () =>
    Effect.gen(function* () {
      const s = yield* setup("OPENCODE IOS")
      yield* s.prompt("hello", { [ORIGIN]: { kind: "session", sessionID: senderID, title: "OPENCODE IOS" } })
      for (const history of [
        yield* SessionHistory.load(s.db, receiverID, "local").pipe(Effect.provide(flag(false))),
        // Without any Config in scope the flag is off too.
        yield* SessionHistory.load(s.db, receiverID, "local"),
      ]) {
        const [message] = history
        if (message?.type !== "user") return yield* Effect.die("Expected a user message")
        expect(message.metadata).toEqual({})
        const { metadata: _, ...plain } = message
        expect(toLLMMessages(history, model)).toEqual(toLLMMessages([plain], model))
      }
    }),
  )

  it.effect("with the flag on, reads the sender's current title over the copy", () =>
    Effect.gen(function* () {
      const s = yield* setup("OPENCODE IOS")
      yield* s.prompt("hello", { [ORIGIN]: { kind: "session", sessionID: senderID, title: "Old title" } })
      yield* s.prompt("tool says hi", { [ORIGIN]: { kind: "tool", name: "ios-train" } })
      yield* s.prompt("typed by a person")
      expect(yield* s.texts(true)).toEqual([
        `${tag("OPENCODE IOS")}\nhello`,
        `<message-origin tool="ios-train"/>\ntool says hi`,
        "typed by a person",
      ])
      yield* s.bus.publish(SessionEvent.Renamed, { sessionID: senderID, title: "iOS HOME" })
      expect((yield* s.texts(true))[0]).toBe(`${tag("iOS HOME")}\nhello`)
      expect(yield* s.texts(false)).toEqual(["hello", "tool says hi", "typed by a person"])
    }),
  )

  it.effect("falls back to the copied title once the sender is deleted, and to unknown without one", () =>
    Effect.gen(function* () {
      const s = yield* setup("OPENCODE IOS")
      yield* s.prompt("copied", { [ORIGIN]: { kind: "session", sessionID: senderID, title: "OPENCODE IOS" } })
      yield* s.prompt("not copied", { [ORIGIN]: { kind: "session", sessionID: senderID } })
      yield* s.db.delete(SessionTable).where(eq(SessionTable.id, senderID)).run()
      expect(yield* s.texts(true)).toEqual([`${tag("OPENCODE IOS")}\ncopied`, `${tag("unknown")}\nnot copied`])
    }),
  )

  it.effect("treats an untitled sender as having no current title", () =>
    Effect.gen(function* () {
      const s = yield* setup(undefined)
      yield* s.prompt("copied", { [ORIGIN]: { kind: "session", sessionID: senderID, title: "Earlier title" } })
      yield* s.prompt("not copied", { [ORIGIN]: { kind: "session", sessionID: senderID } })
      expect(yield* s.texts(true)).toEqual([`${tag("Earlier title")}\ncopied`, `${tag("unknown")}\nnot copied`])
    }),
  )

  it.effect("leaves a value that doesn't decode unrendered", () =>
    Effect.gen(function* () {
      const s = yield* setup("OPENCODE IOS")
      yield* s.prompt("hello", { [ORIGIN]: { kind: "session", sessionID: 42 } })
      expect(yield* s.texts(true)).toEqual(["hello"])
    }),
  )

  it.effect("rebuilds a compaction's recent text with the current title and one [User]: line", () =>
    Effect.gen(function* () {
      const s = yield* setup("OPENCODE IOS")
      const keepID = yield* s.prompt("kept", { [ORIGIN]: { kind: "session", sessionID: senderID } })
      yield* s.bus.publish(
        SessionEvent.Compaction.Ended,
        { sessionID: receiverID, reason: "manual", model, text: "summary" },
        { metadata: { keepID } },
      )
      const recent = (enabled: boolean) =>
        SessionHistory.load(s.db, receiverID, "latest").pipe(
          Effect.provide(flag(enabled)),
          Effect.map((history) => {
            const compaction = history[0]
            return compaction?.type === "compaction" && compaction.status === "completed"
              ? SessionRecentText.kept(compaction)
              : undefined
          }),
        )
      expect(yield* recent(true)).toBe(`[User]: ${tag("OPENCODE IOS")}\nkept`)
      expect(yield* recent(false)).toBe("[User]: kept")
    }),
  )
})
