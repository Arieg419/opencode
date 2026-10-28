export * as MessageContext from "./message-context.js"

import { Session } from "@opencode/schema/session"
import { Option, Schema } from "effect"

/** Who sent a user message, stored under {@link ORIGIN} in its metadata. */
export const ORIGIN = "opencode.origin"

export const SessionOrigin = Schema.Struct({
  kind: Schema.Literal("session"),
  sessionID: Session.ID,
  // The sender's title when the message arrived, read only when the sender no longer has one.
  title: Schema.optional(Schema.String),
})
export type SessionOrigin = typeof SessionOrigin.Type

export const ToolOrigin = Schema.Struct({
  kind: Schema.Literal("tool"),
  name: Schema.String,
})

export const Origin = Schema.Union([SessionOrigin, ToolOrigin])
export type Origin = typeof Origin.Type

export const decodeOrigin = Schema.decodeUnknownOption(Origin)

/** Prompt metadata for a message one session sends another. */
export const fromSession = (sessionID: Session.ID) => ({
  [ORIGIN]: { kind: "session", sessionID } satisfies Origin,
})

type Formatter = (value: unknown) => string | undefined

const escapeAttribute = (value: string) =>
  value.replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")

const tag = (name: string, attributes: Record<string, string>) =>
  `<${name} ${Object.entries(attributes)
    .map(([key, value]) => `${key}="${escapeAttribute(value)}"`)
    .join(" ")}/>`

const origin: Formatter = (value) => {
  const decoded = Option.getOrUndefined(decodeOrigin(value))
  if (decoded === undefined) return undefined
  if (decoded.kind === "tool") return tag("message-origin", { tool: decoded.name })
  return tag("message-origin", { session: decoded.sessionID, title: decoded.title ?? "unknown" })
}

// Metadata keys the agent reads, each with a pure formatter. History drops them all when
// `experimental.message_context` is off, so a formatter never needs to check the flag.
const formatters: Record<string, Formatter> = {
  [ORIGIN]: origin,
}

export const keys = Object.keys(formatters)

/** The lines the agent reads before a user message's text, or "" when its metadata has none. */
export const format = (metadata?: Readonly<Record<string, unknown>>) =>
  metadata === undefined
    ? ""
    : Object.entries(formatters)
        .flatMap(([key, line]) => line(metadata[key]) ?? [])
        .join("\n")

/** A user message's text with its context lines before it. */
export const withText = (metadata: Readonly<Record<string, unknown>> | undefined, text: string) =>
  [format(metadata), text].filter(Boolean).join("\n")
