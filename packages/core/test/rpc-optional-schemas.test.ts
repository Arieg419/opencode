import { expect } from "bun:test"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Location } from "@opencode/core/location"
import { Rpc } from "@opencode/core/rpc"
import { AbsolutePath } from "@opencode/core/schema"
import { Plugin } from "@opencode/plugin"
import { fromPromise } from "@opencode/plugin/promise/adapter"
import { Effect, Layer, Schema } from "effect"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"
import { host } from "./plugin/host"

const it = testEffect(
  AppNodeBuilder.build(Rpc.node, [
    Location.node.replace(Layer.succeed(Location.Service, location({ directory: AbsolutePath.make("/rpc-project") }))),
  ]),
)

const EffectOneSided = Rpc.define({
  id: "effect-one-sided",
  methods: {
    status: { output: Schema.Struct({ ok: Schema.Boolean }) },
    notify: { input: Schema.Struct({ text: Schema.String }) },
    reset: {},
  },
})

const PortableOneSided = Rpc.define({
  id: "portable-one-sided",
  methods: {
    status: {
      output: {
        type: "object",
        properties: { ok: { type: "boolean" }, items: { type: "number" } },
        required: ["ok", "items"],
        additionalProperties: false,
      },
    },
    notify: {
      input: {
        type: "object",
        properties: { text: { type: "string" } },
        required: ["text"],
        additionalProperties: false,
      },
    },
    reset: {},
  },
})

it.effect("calls Effect RPC methods that omit input, output, or events", () =>
  Effect.gen(function* () {
    const rpc = yield* Rpc.Service
    const seen: string[] = []
    const receivedInputs: unknown[] = []

    const registration = yield* rpc.register(EffectOneSided, {
      status: (input) => {
        receivedInputs.push(input)
        return Effect.succeed({ ok: true })
      },
      notify: (input) => {
        seen.push(input.text)
        return Effect.void
      },
      reset: () => {
        seen.length = 0
        return Effect.void
      },
    })

    const client = rpc.client(EffectOneSided)
    expect(yield* client.status()).toEqual({ ok: true })
    expect(receivedInputs).toEqual([undefined])

    expect(yield* client.notify({ text: "hello" })).toBeUndefined()
    expect(seen).toEqual(["hello"])

    expect(yield* client.reset()).toBeUndefined()
    expect(seen).toEqual([])

    if (false) {
      // @ts-expect-error Methods without an input schema reject arguments.
      void client.status({ unexpected: true })
      // @ts-expect-error Definitions without events reject event emission.
      void registration.events.emit("updated", {})
      // @ts-expect-error Definitions without events reject event subscriptions.
      void client.events.subscribe("updated")
    }
  }),
)

it.effect("rejects unexpected input on input-less methods and discards stray output on output-less methods", () =>
  Effect.gen(function* () {
    const rpc = yield* Rpc.Service

    yield* rpc.register(EffectOneSided, {
      status: () => Effect.succeed({ ok: true }),
      // Effect<A> is covariant and assignable to Effect<void>; Schema.Void discards the value so it never leaks.
      notify: () => Effect.succeed({ leaked: "internal" }),
      reset: () => Effect.void,
    })

    const badInput = yield* rpc.call("effect-one-sided", "status", { unexpected: 1 }).pipe(Effect.flip)
    expect(badInput.type).toBe("rpc.invalid_input")

    expect(yield* rpc.call("effect-one-sided", "notify", { text: "hi" })).toBeUndefined()
  }),
)

it.effect("maps malformed non-object schemas from untyped JS plugins to rpc.invalid_input instead of throwing", () =>
  Effect.gen(function* () {
    const rpc = yield* Rpc.Service
    const malformed = Rpc.define({
      id: "malformed",
      methods: {
        bad: {
          // @ts-expect-error Intentionally pass a non-object schema as an untyped JS plugin would.
          input: 123,
          output: { type: "object" },
        },
      },
    })

    yield* rpc.register(malformed, {
      bad: () => Effect.succeed({}),
    })

    const result = yield* rpc.call("malformed", "bad", {}).pipe(Effect.flip)
    expect(result.type).toBe("rpc.invalid_input")
  }),
)

it.effect("supports Promise plugins registering and calling JSON Schema RPCs without input, output, or events", () =>
  Effect.gen(function* () {
    const rpc = yield* Rpc.Service
    const pluginHost = host({ rpc: Object.assign(rpc.client, { register: rpc.register }) })

    const items: string[] = []
    let statusSnapshot: unknown
    const TextInput = Schema.Struct({ text: Schema.String })
    const plugin = fromPromise(
      Plugin.define({
        id: "portable-one-sided-plugin",
        async setup(ctx) {
          const registration = await ctx.rpc.register(PortableOneSided, {
            status: async () => ({ ok: true, items: items.length }),
            notify: async (input) => {
              const payload = Schema.decodeUnknownSync(TextInput)(input)
              items.push(payload.text)
            },
            reset: async () => {
              items.length = 0
            },
          })

          const local = ctx.rpc(PortableOneSided)
          await local.notify({ text: "first" })
          statusSnapshot = await local.status()
          await local.reset()

          if (false) {
            // @ts-expect-error Input-less methods do not accept dummy {} arguments.
            await local.status({})
            // @ts-expect-error Definitions without events cannot emit events.
            await registration.events.emit("updated", {})
            // @ts-expect-error Definitions without events cannot subscribe to events.
            local.events.on("updated", () => {})
            await ctx.rpc.register(PortableOneSided, {
              status: async () => ({ ok: true, items: 0 }),
              notify: async () => {},
              // @ts-expect-error Output-less Promise handlers must return void.
              reset: async () => ({ leaked: true }),
            })
          }
        },
      }),
    )

    yield* plugin.effect(pluginHost)
    expect(statusSnapshot).toEqual({ ok: true, items: 1 })
    expect(items).toEqual([])
    expect(yield* rpc.call("portable-one-sided", "status", undefined)).toEqual({ ok: true, items: 0 })
  }),
)
