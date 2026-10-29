import { expect } from "bun:test"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Location } from "@opencode/core/location"
import { Rpc } from "@opencode/core/rpc"
import { AbsolutePath } from "@opencode/core/schema"
import { Effect, Layer, Schema } from "effect"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"

const it = testEffect(
  AppNodeBuilder.build(Rpc.node, [
    Location.node.replace(Layer.succeed(Location.Service, location({ directory: AbsolutePath.make("/rpc-project") }))),
  ]),
)

const Partial = Rpc.define({
  id: "partial",
  methods: {
    status: { output: { type: "object" } },
    record: { input: Schema.Struct({ text: Schema.String }) },
    ping: {},
  },
  events: {},
})

it.effect("calls methods that leave out their input or output schema", () =>
  Effect.gen(function* () {
    const rpc = yield* Rpc.Service
    const received: unknown[] = []
    yield* rpc.register(Partial, {
      status: (input) => {
        received.push(input)
        return Effect.succeed({ ok: true })
      },
      record: (input) => {
        received.push(input)
        return Effect.void
      },
      ping: () => Effect.void,
    })
    const client = rpc.client(Partial)

    expect(yield* client.status()).toEqual({ ok: true })
    expect(yield* client.record({ text: "kept" })).toBeUndefined()
    expect(yield* client.ping()).toBeUndefined()
    // @ts-expect-error a method without an input schema takes no input
    yield* client.status({ ignored: true })
    expect(received).toEqual([undefined, { text: "kept" }, undefined])
  }),
)

it.effect("drops values on the side without a schema", () =>
  Effect.gen(function* () {
    const rpc = yield* Rpc.Service
    const Leaky = { ...Partial, id: "leaky" }
    yield* rpc.register(Leaky, {
      status: (input) => Effect.succeed({ dropped: input === undefined }),
      record: () => Effect.succeed("leaked"),
      ping: () => Effect.void,
    })

    expect(yield* rpc.call("leaky", "status", { secret: true })).toEqual({ dropped: true })
    expect(yield* rpc.call("leaky", "record", { text: "kept" })).toBeUndefined()
  }),
)

it.effect("still validates the schema a method declares", () =>
  Effect.gen(function* () {
    const rpc = yield* Rpc.Service
    yield* rpc.register(Partial, {
      status: () => Effect.succeed({ ok: true }),
      record: () => Effect.void,
      ping: () => Effect.void,
    })

    const failure = yield* rpc.call("partial", "record", { text: 42 }).pipe(Effect.flip)
    expect(failure.type).toBe("rpc.invalid_input")
  }),
)
