export * as ClientCertificate from "./client-certificate.js"

import path from "path"
import { Context, Effect, Layer } from "effect"
import { FetchHttpClient, HttpClient, HttpClientError } from "effect/http"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { FSUtil } from "@opencode/util/fs-util"
import { Global } from "@opencode/util/global"
import { AppProcess } from "@opencode/util/process"
import { Config } from "./config.js"
import { ConfigDiscovery } from "./config/discovery.js"
import { Wildcard } from "./util/wildcard.js"

export interface Rule {
  readonly hosts: readonly string[]
  readonly cert: string
  readonly key: string
  readonly passphrase?: string
}

export interface Interface {
  /** The machine's client certificate rules, reloaded at most once a minute. */
  readonly rules: () => Effect.Effect<readonly Rule[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ClientCertificate") {}

export interface Options {
  /** System configuration directory. A custom one replaces platform discovery, so it also skips MDM preferences. */
  readonly system?: string
}

export const layer = (options?: Options) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const global = yield* Global.Service
      const context = yield* Effect.context<FSUtil.Service | AppProcess.Service>()
      const rules = yield* Effect.cachedWithTTL(
        load(global, options?.system).pipe(
          Effect.provide(context),
          Effect.catch((error) =>
            Effect.logWarning("failed to load client certificates", { error }).pipe(Effect.as([])),
          ),
        ),
        "1 minute",
      )
      return Service.of({ rules: () => rules })
    }),
  )

/**
 * Only machine-level sources apply: a client certificate is the machine's identity, so a repository's config must not
 * choose one. Managed preferences outrank the system directory, which outranks the user's global config, and the
 * highest-ranked document that sets `network.client_certificates` replaces the others.
 */
const load = Effect.fn("ClientCertificate.load")(function* (global: Global.Interface, system: string | undefined) {
  const plist = system === undefined ? yield* ConfigDiscovery.preferences() : undefined
  const documents = [
    ...(yield* Effect.forEach(ConfigDiscovery.names, (name) => Config.loadFile(path.join(global.config, name)))),
    ...(yield* Effect.forEach(ConfigDiscovery.names, (name) =>
      Config.loadFile(path.join(system ?? ConfigDiscovery.systemDirectory(), name)),
    )),
    ...(plist ? yield* Config.loadPreferences(plist) : []),
  ]
  const source = documents.findLast((document) => document?.info.network?.client_certificates !== undefined)
  if (!source) return []
  const directory = path.dirname(source.path ?? global.config)
  const resolve = (file: string) =>
    file.startsWith("~/") ? path.join(global.home, file.slice(2)) : path.resolve(directory, file)
  return (source.info.network?.client_certificates ?? []).map(
    (entry): Rule => ({
      hosts: entry.hosts.map((host) => host.toLowerCase()),
      cert: resolve(entry.cert),
      key: resolve(entry.key),
      passphrase: entry.passphrase,
    }),
  )
})

/** The first rule naming the URL's host; plain-HTTP URLs never match. */
export function match(rules: readonly Rule[], url: string) {
  if (!URL.canParse(url)) return
  const target = new URL(url)
  if (target.protocol !== "https:" && target.protocol !== "wss:") return
  const host = target.hostname.toLowerCase()
  return rules.find((rule) => rule.hosts.some((pattern) => Wildcard.match(host, pattern)))
}

export function configured(options?: Options) {
  return makeGlobalNode({ service: Service, layer: layer(options), deps: [FSUtil.node, Global.node, AppProcess.node] })
}

export const node = configured()

/** Fetch-backed HTTP client that presents the matching client certificate on each HTTPS request. */
export const httpClientLayer = Layer.effect(
  HttpClient.HttpClient,
  Effect.gen(function* () {
    const certificates = yield* Service
    const client = yield* HttpClient.HttpClient
    return HttpClient.transform(client, (effect, request) =>
      Effect.gen(function* () {
        const rule = match(yield* certificates.rules(), request.url)
        if (!rule) return yield* effect
        if (typeof Bun === "undefined")
          return yield* new HttpClientError.HttpClientError({
            reason: new HttpClientError.TransportError({
              request,
              description: `A client certificate is configured for ${new URL(request.url).hostname}, but this runtime cannot present one`,
            }),
          })
        const fetch = yield* FetchHttpClient.Fetch
        return yield* effect.pipe(
          Effect.provideService(
            FetchHttpClient.Fetch,
            Object.assign(
              (input: Parameters<typeof globalThis.fetch>[0], init?: BunFetchRequestInit) =>
                fetch(input, {
                  ...init,
                  tls: {
                    ...init?.tls,
                    cert: Bun.file(rule.cert),
                    key: Bun.file(rule.key),
                    passphrase: rule.passphrase,
                  },
                  // Fetch would follow a redirect with the same TLS options, presenting the certificate to whatever
                  // host the server names. Only callers that handle redirects themselves see the 3xx.
                  redirect: init?.redirect === "manual" ? "manual" : "error",
                }),
              { preconnect: fetch.preconnect },
            ),
          ),
        )
      }),
    )
  }),
).pipe(Layer.provide(FetchHttpClient.layer))

export const httpClient = makeGlobalNode({ service: HttpClient.HttpClient, layer: httpClientLayer, deps: [node] })
