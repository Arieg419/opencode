import path from "path"
import fs from "fs/promises"
import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import { Effect, Layer, Option, Schema } from "effect"
import { ConfigNetwork } from "@opencode/schema/config/network"
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http"
import { RequestExecutor } from "@opencode/ai/route"
import { ClientCertificate } from "@opencode/core/client-certificate"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNodePlatform } from "@opencode/core/effect/app-node-platform"
import { httpClient } from "@opencode/util/effect/app-node-platform"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Global } from "@opencode/util/global"
import { withEnv } from "./fixture/env"
import { tmpdir } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

const it = testEffect(Layer.empty)
// The certificates are generated per run; a runner without OpenSSL lacks the capability, not the behavior.
const itWithOpenSSL = Bun.which("openssl") ? it.live : it.live.skip

let root: Awaited<ReturnType<typeof tmpdir>>
let ca = ""
let server: Bun.Server<undefined>

beforeAll(async () => {
  root = await tmpdir()
  await fs.mkdir(path.join(root.path, "system"))
  await fs.mkdir(path.join(root.path, "global"))
  if (!Bun.which("openssl")) return
  const certs = path.join(root.path, "system", "certs")
  await fs.mkdir(certs)
  const openssl = (...args: string[]) =>
    Bun.$`openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -days 2 ${args}`.cwd(certs).quiet()
  await openssl("-nodes", "-keyout", "ca.key", "-out", "ca.pem", "-subj", "/CN=OpenCode Test CA")
  await Promise.all([
    openssl(
      ...["-CA", "ca.pem", "-CAkey", "ca.key", "-nodes", "-keyout", "server.key", "-out", "server.pem"],
      ...["-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"],
    ),
    openssl(
      ...["-CA", "ca.pem", "-CAkey", "ca.key", "-passout", "pass:secret", "-keyout", "client.key"],
      ...["-out", "client.pem", "-subj", "/CN=client", "-addext", "extendedKeyUsage=clientAuth"],
    ),
  ])
  await Bun.write(path.join(certs, "passphrase"), "secret\n")
  ca = await Bun.file(path.join(certs, "ca.pem")).text()
  server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    tls: {
      cert: Bun.file(path.join(certs, "server.pem")),
      key: Bun.file(path.join(certs, "server.key")),
      ca,
      requestCert: true,
      rejectUnauthorized: true,
    },
    fetch: (request) =>
      new URL(request.url).pathname === "/redirect"
        ? Response.redirect(`https://127.0.0.1:${server.port}/`, 302)
        : new Response("ok"),
  })
})

afterAll(async () => {
  await server?.stop(true)
  await root[Symbol.asyncDispose]()
})

const write = (directory: string, config: unknown) =>
  Effect.promise(() => Bun.write(path.join(root.path, directory, "opencode.json"), JSON.stringify(config)))

// The key is encrypted, so a request only succeeds when the passphrase reaches the TLS options too.
const certificate = (hosts: string[]) => ({
  network: {
    client_certificates: [
      { hosts, cert: "certs/client.pem", key: "certs/client.key", passphrase_file: "certs/passphrase" },
    ],
  },
})

const layer = () =>
  AppNodeBuilder.build(LayerNode.group([LayerNodePlatform.requestExecutor, httpClient, ClientCertificate.node]), [
    ClientCertificate.node.replace(ClientCertificate.configured({ system: path.join(root.path, "system") })),
    Global.node.replace(
      Global.layerWith({ config: path.join(root.path, "global"), home: path.join(root.path, "home") }),
    ),
  ])

// The test CA only verifies the server; trust for real gateways comes from the system store or NODE_EXTRA_CA_CERTS.
const trusted = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.provideService(
      FetchHttpClient.Fetch,
      Object.assign(
        (input: Parameters<typeof fetch>[0], init?: BunFetchRequestInit) =>
          fetch(input, { ...init, tls: { ca, ...init?.tls } }),
        { preconnect: fetch.preconnect },
      ),
    ),
  )

/** A model request, the only kind that presents client certificates. */
const get = (host: string, pathname = "/") =>
  Effect.gen(function* () {
    const executor = yield* RequestExecutor.Service
    const response = yield* executor.execute(HttpClientRequest.get(`https://${host}:${server?.port ?? 443}${pathname}`))
    return yield* response.text
  }).pipe(trusted)

describe("ClientCertificate", () => {
  test("accepts hostnames and rejects URLs, ports, and wildcard-only patterns", () => {
    const valid = (host: string) =>
      Option.isSome(
        Schema.decodeUnknownOption(ConfigNetwork.ClientCertificate)({ hosts: [host], cert: "c.pem", key: "k.pem" }),
      )
    expect(["llm.corp.test", "*.corp.test", "llm-?.corp.test", "10.0.0.1", "[::1]"].filter(valid)).toHaveLength(5)
    expect(
      ["https://llm.corp.test", "llm.corp.test:8443", "llm.corp.test/v1", "*", "*.*", "::1"].filter(valid),
    ).toEqual([])
  })

  itWithOpenSSL("presents the certificate to a matching host", () =>
    Effect.gen(function* () {
      yield* write("system", certificate(["LOCALHOST"]))
      expect(yield* get("localhost")).toBe("ok")
    }).pipe(Effect.provide(layer())),
  )

  itWithOpenSSL("does not present the certificate to other hosts", () =>
    Effect.gen(function* () {
      yield* write("system", certificate(["localhost"]))
      expect(yield* Effect.flip(get("127.0.0.1"))).toBeDefined()
    }).pipe(Effect.provide(layer())),
  )

  itWithOpenSSL("does not present the certificate on requests other than model requests", () =>
    Effect.gen(function* () {
      yield* write("system", certificate(["localhost"]))
      const client = yield* HttpClient.HttpClient
      expect(yield* Effect.flip(client.get(`https://localhost:${server.port}/`).pipe(trusted))).toBeDefined()
    }).pipe(Effect.provide(layer())),
  )

  it.live("refuses to send the certificate through an HTTPS proxy", () =>
    withEnv(
      { HTTPS_PROXY: "https://proxy.test", https_proxy: undefined, NO_PROXY: undefined, no_proxy: undefined },
      () =>
        Effect.gen(function* () {
          yield* write("system", certificate(["llm.corp.test"]))
          const error = yield* Effect.flip(get("llm.corp.test"))
          expect(String(error)).toContain("NO_PROXY")
        }),
    ).pipe(Effect.provide(layer())),
  )

  itWithOpenSSL("fails instead of following a redirect with the certificate", () =>
    Effect.gen(function* () {
      yield* write("system", certificate(["localhost", "127.0.0.1"]))
      const error = yield* Effect.flip(get("localhost", "/redirect"))
      expect(String(error.cause)).toContain("Redirect")
    }).pipe(Effect.provide(layer())),
  )

  it.live("lets system configuration replace the user's certificates", () =>
    Effect.gen(function* () {
      yield* write("global", {
        network: { client_certificates: [{ hosts: ["user.test"], cert: "~/user.pem", key: "user.key" }] },
      })
      yield* write("system", {
        network: {
          client_certificates: [
            { hosts: ["*.corp.test", "[0:0:0:0:0:0:0:1]"], cert: "/etc/corp.pem", key: "corp.key" },
          ],
        },
      })
      const certificates = yield* ClientCertificate.Service
      const rules = yield* certificates.rules()
      expect(rules).toEqual([
        {
          hosts: ["*.corp.test", "[::1]"],
          cert: path.resolve(root.path, "/etc/corp.pem"),
          key: path.join(root.path, "system", "corp.key"),
          passphraseFile: undefined,
        },
      ])
      expect(ClientCertificate.match(rules, "https://api.corp.test/v1")).toBe(rules[0])
      expect(ClientCertificate.match(rules, "https://API.corp.test./v1")).toBe(rules[0])
      expect(ClientCertificate.match(rules, "https://[::1]/")).toBe(rules[0])
      expect(ClientCertificate.match(rules, "http://api.corp.test/v1")).toBeUndefined()
      expect(ClientCertificate.match(rules, "https://user.test/v1")).toBeUndefined()
      const specific = { ...rules[0], hosts: ["llm.corp.test"] }
      expect(ClientCertificate.match([...rules, specific], "https://llm.corp.test/v1")).toBe(specific)
    }).pipe(Effect.provide(layer())),
  )

  it.live("resolves the user's certificates when no system configuration sets any", () =>
    Effect.gen(function* () {
      yield* write("global", {
        network: { client_certificates: [{ hosts: ["user.test"], cert: "~/user.pem", key: "user.key" }] },
      })
      yield* write("system", {})
      const certificates = yield* ClientCertificate.Service
      expect(yield* certificates.rules()).toEqual([
        {
          hosts: ["user.test"],
          cert: path.join(root.path, "home", "user.pem"),
          key: path.join(root.path, "global", "user.key"),
          passphraseFile: undefined,
        },
      ])
    }).pipe(Effect.provide(layer())),
  )
})
