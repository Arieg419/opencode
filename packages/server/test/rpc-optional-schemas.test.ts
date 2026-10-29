import { expect } from "bun:test"
import { OpenCode } from "@opencode/client"
import { SdkPlugins } from "@opencode/core/plugin/sdk"
import { Plugin } from "@opencode/plugin"
import { fromPromise } from "@opencode/plugin/promise/adapter"
import { Rpc } from "@opencode/plugin/rpc"
import { Context, Effect, Layer, Schema } from "effect"
import { HttpClient, HttpClientResponse, HttpEffect, HttpRouter, HttpServer } from "effect/http"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { createEmbeddedRoutes } from "../src/routes"

const Vibeguard = Rpc.define({
  id: "vibeguard",
  methods: {
    status: {
      output: {
        type: "object",
        properties: { ok: { type: "boolean" }, count: { type: "number" } },
        required: ["ok", "count"],
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

it.live("dispatches HTTP and SDK RPC calls for methods without input, output, or events", () =>
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
    const messages: string[] = []
    const sdk = Context.get(context, SdkPlugins.Service)
    yield* sdk.register(
      fromPromise(
        Plugin.define({
          id: "vibeguard-plugin",
          async setup(ctx) {
            await ctx.rpc.register(Vibeguard, {
              status: async () => ({ ok: true, count: messages.length }),
              notify: async (input) => {
                const payload = Schema.decodeUnknownSync(Schema.Struct({ text: Schema.String }))(input)
                messages.push(payload.text)
              },
              reset: async () => {
                messages.length = 0
              },
            })
          },
        }),
      ),
    )

    const webHandler = Context.get(context, HttpRouter.HttpRouter)
      .asHttpEffect()
      .pipe(HttpEffect.toWebHandlerWith(context))

    const post = (method: string, body: unknown) => {
      const url = new URL(`/api/rpc/vibeguard/${method}`, "http://opencode.local")
      url.searchParams.set("location[directory]", directory.path)
      return Effect.promise(async () => {
        const response = await webHandler(
          new Request(url, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(body),
          }),
        )
        return { status: response.status, json: await response.json() }
      })
    }

    // Exact #59755 HTTP reproduction: `{}` omits `input` on an output-only method.
    expect(yield* post("status", {})).toEqual({
      status: 200,
      json: { output: { ok: true, count: 0 } },
    })

    // Non-undefined input on an input-less method is rejected as 400 rpc.invalid_input.
    const invalidInput = yield* post("status", { input: {} })
    expect(invalidInput.status).toBe(400)
    expect(invalidInput.json).toMatchObject({ _tag: "RpcError", type: "rpc.invalid_input" })

    // Output-less methods omit `output` in the HTTP response body (`{}`).
    expect(yield* post("notify", { input: { text: "alpha" } })).toEqual({
      status: 200,
      json: {},
    })

    // Promise SDK client calls zero-arg and void methods directly.
    const promiseClient = OpenCode.make({
      baseUrl: "http://opencode.local",
      fetch: Object.assign(
        (input: RequestInfo | URL, init?: RequestInit) => webHandler(new Request(input, init)),
        { preconnect: globalThis.fetch.preconnect },
      ),
    })
    const promiseRpc = promiseClient.rpc(Vibeguard)
    expect(yield* Effect.promise(() => promiseRpc.status(undefined, { location: { directory: directory.path } }))).toEqual({
      ok: true,
      count: 1,
    })
    expect(
      yield* Effect.promise(() => promiseRpc.notify({ text: "beta" }, { location: { directory: directory.path } })),
    ).toBeUndefined()
    expect(yield* Effect.promise(() => promiseRpc.status())).toEqual({ ok: true, count: 2 })
    expect(yield* Effect.promise(() => promiseRpc.reset())).toBeUndefined()

    // Effect SDK client calls the same contract directly.
    const httpClient = HttpClient.make((request) =>
      Effect.promise(async () => {
        const body =
          request.body._tag === "Uint8Array"
            ? Buffer.from(request.body.body)
            : request.body._tag === "Raw"
              ? String(request.body.body)
              : undefined
        const response = await webHandler(
          new Request(request.url, {
            method: request.method,
            headers: request.headers,
            body,
          }),
        )
        return HttpClientResponse.fromWeb(request, response)
      }),
    )
    const effectSdk = yield* Effect.promise(() => import("@opencode/client/effect"))
    const effectClient = yield* effectSdk.OpenCode.make({
      baseUrl: "http://opencode.local",
    }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient))
    const effectRpc = effectClient.rpc(Vibeguard)
    expect(yield* effectRpc.notify({ text: "gamma" })).toBeUndefined()
    expect(yield* effectRpc.status()).toEqual({ ok: true, count: 1 })
    expect(yield* effectRpc.reset()).toBeUndefined()
    expect(yield* effectRpc.status()).toEqual({ ok: true, count: 0 })
  }),
)
