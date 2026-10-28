export * as ToolPartial from "./partial.js"

import type { Content } from "@opencode/schema/tool"
import { Context, Effect } from "effect"

/** Partial content travels inline with the failure, so producers keep its text within this many UTF-8 bytes. */
export const MAX_BYTES = 16 * 1024

type Recorder = (content: ReadonlyArray<Content>) => void

// The session runner sets this around each local tool call it starts.
const Current = Context.Reference<Recorder | undefined>("@opencode/ToolPartial", { defaultValue: () => undefined })

/**
 * Records what the running call has produced so far, replacing an earlier record. If the call is interrupted, the
 * session runner sends this content with the failure instead of an empty result.
 */
export const record = (content: ReadonlyArray<Content>) => Current.useSync((recorder) => recorder?.(content))

/** Sends the records of the calls run by `effect` to `recorder`. */
export const provide =
  (recorder: Recorder) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.provideService(effect, Current, recorder)

/** Keeps a call made from within another call from replacing the outer call's record. */
export const isolate = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.provideService(effect, Current, undefined)
