import { Schema } from "effect"
import { Rpc, RpcGroup } from "effect/rpc"

export const HexDictationStartResultSchema = Schema.Union([
  Schema.Struct({ status: Schema.Literal("recording") }),
  Schema.Struct({
    status: Schema.Literal("error"),
    code: Schema.Literals(["unsupported", "not-installed", "not-ready", "busy", "failed"]),
    message: Schema.optional(Schema.String),
  }),
])

export const HexDictationFinishResultSchema = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("transcribed"),
    transcript: Schema.String,
  }),
  Schema.Struct({ status: Schema.Literal("empty") }),
  Schema.Struct({
    status: Schema.Literal("error"),
    code: Schema.Literals(["not-active", "failed"]),
    message: Schema.optional(Schema.String),
  }),
])

export const HexDictationStart = Rpc.make("HexDictationStart", {
  success: HexDictationStartResultSchema,
})

export const HexDictationFinish = Rpc.make("HexDictationFinish", {
  success: HexDictationFinishResultSchema,
})

export const HexDictationCancel = Rpc.make("HexDictationCancel")

export const HexRpcs = RpcGroup.make(HexDictationStart, HexDictationFinish, HexDictationCancel)
