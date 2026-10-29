import { expect } from "bun:test"
import { SdkPlugins } from "@opencode/core/plugin/sdk"
import { define } from "@opencode/plugin/effect/plugin"
import { Rpc } from "@opencode/schema/rpc"
import { Context, Effect, Layer } from "effect"
import { HttpEffect, HttpRouter, HttpServer } from "effect/http"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { createEmbeddedRoutes } from "../src/routes"

const Partial = Rpc.define({
  id: "partial",
  methods: {
    status: { output: { type: "object" } },
    record: { input: { type: "object" } },
  },
  events: {},
})

it.live("calls methods without an input or output schema over HTTP", () =>
  Effect.gen(function* () {
    const directory = yield* tmpdirScoped()
    const context = yield* Layer.build(
      createEmbeddedRoutes({
        database: { path: ":memory:" },
        models: { fetch: false },
        config: { directory: directory.path, project: false, content: "{}" },
        fs: { filewatcher: false },
      }).pipe(Layer.provide(HttpServer.layerServices)),
    )
    const sdk = Context.get(context, SdkPlugins.Service)
    yield* sdk.register(
      define({
        id: "partial-rpc",
        effect: (ctx) =>
          ctx.rpc
            .register(Partial, {
              status: () => Effect.succeed({ ok: true }),
              record: () => Effect.void,
            })
            .pipe(Effect.asVoid, Effect.orDie),
      }),
    )
    const handler = Context.get(context, HttpRouter.HttpRouter)
      .asHttpEffect()
      .pipe(HttpEffect.toWebHandlerWith(context))
    const call = (method: string, body: unknown) => {
      const url = new URL(`/api/rpc/partial/${method}`, "http://opencode.local")
      url.searchParams.set("location[directory]", directory.path)
      return Effect.promise(async () => {
        const response = await handler(
          new Request(url, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(body),
          }),
        )
        return { status: response.status, body: await response.json() }
      })
    }

    expect(yield* call("status", {})).toEqual({ status: 200, body: { output: { ok: true } } })
    expect(yield* call("status", { input: {} })).toEqual({ status: 200, body: { output: { ok: true } } })
    expect(yield* call("record", { input: { text: "hello" } })).toEqual({ status: 200, body: {} })
  }),
)
