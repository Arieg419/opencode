import path from "path"
import fs from "fs/promises"
import { afterAll, beforeAll, describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import { FetchHttpClient, HttpClient } from "effect/http"
import { ClientCertificate } from "@opencode/core/client-certificate"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Global } from "@opencode/util/global"
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
    client_certificates: [{ hosts, cert: "certs/client.pem", key: "certs/client.key", passphrase: "secret" }],
  },
})

const layer = () =>
  AppNodeBuilder.build(LayerNode.group([ClientCertificate.httpClient, ClientCertificate.node]), [
    ClientCertificate.node.replace(ClientCertificate.configured({ system: path.join(root.path, "system") })),
    Global.node.replace(
      Global.layerWith({ config: path.join(root.path, "global"), home: path.join(root.path, "home") }),
    ),
  ])

// The test CA only verifies the server; trust for real gateways comes from the system store or NODE_EXTRA_CA_CERTS.
const get = (host: string, pathname = "/") =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient
    const response = yield* client.get(`https://${host}:${server.port}${pathname}`)
    return yield* response.text
  }).pipe(Effect.provideService(FetchHttpClient.RequestInit, { tls: { ca } } as BunFetchRequestInit))

describe("ClientCertificate", () => {
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
        network: { client_certificates: [{ hosts: ["*.corp.test"], cert: "/etc/corp.pem", key: "corp.key" }] },
      })
      const certificates = yield* ClientCertificate.Service
      const rules = yield* certificates.rules()
      expect(rules).toEqual([
        {
          hosts: ["*.corp.test"],
          cert: path.resolve(root.path, "/etc/corp.pem"),
          key: path.join(root.path, "system", "corp.key"),
          passphrase: undefined,
        },
      ])
      expect(ClientCertificate.match(rules, "https://api.corp.test/v1")).toBe(rules[0])
      expect(ClientCertificate.match(rules, "http://api.corp.test/v1")).toBeUndefined()
      expect(ClientCertificate.match(rules, "https://user.test/v1")).toBeUndefined()
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
          passphrase: undefined,
        },
      ])
    }).pipe(Effect.provide(layer())),
  )
})
