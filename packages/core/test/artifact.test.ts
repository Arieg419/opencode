import fs from "node:fs/promises"
import path from "node:path"
import { describe, expect } from "bun:test"
import { Effect, Layer, Stream } from "effect"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Artifact } from "@opencode/core/artifact"
import { Config } from "@opencode/core/config"
import { FileAccess } from "@opencode/core/file-access"
import { Image } from "@opencode/core/image"
import { Location } from "@opencode/core/location"
import { Permission } from "@opencode/core/permission"
import { Session } from "@opencode/core/session"
import { Tool } from "@opencode/core/tool"
import { ArtifactTools } from "@opencode/core/tool/plugin/artifact"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Global } from "@opencode/util/global"
import { Document, Info } from "@opencode/schema/config"
import { ConfigExperimental } from "@opencode/schema/config/experimental"
import { AbsolutePath } from "@opencode/schema/schema"
import { tmpdirScoped } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"
import { imagePassthrough } from "./lib/image"
import { permissionLayer } from "./lib/permission"
import { codeModeListings, executeTool, registerToolPlugin, toolIdentity } from "./lib/tool"

const toolPaths = [
  "opencode.artifact_create",
  "opencode.artifact_read",
  "opencode.artifact_update",
  "opencode.artifact_delete",
  "opencode.artifact_list",
]

const buildLayer = (artifactsEnabled: boolean, dir: string) => {
  const entries = artifactsEnabled
    ? [
        new Document({
          type: "document",
          info: new Info({
            experimental: new ConfigExperimental.Info({ artifacts: true }),
          }),
        }),
      ]
    : []
  const ref = Location.Ref.make({ directory: AbsolutePath.make(dir) })
  return AppNodeBuilder.build(
    LayerNode.group([
      Artifact.node,
      Config.node,
      FileAccess.node,
      Global.node,
      Location.node,
      Permission.node,
      Session.node,
      Tool.node,
    ]),
    [
      Config.node.replace(Config.testLayer(entries)),
      Global.node.replace(Global.layerWith({ data: path.join(dir, ".data") })),
      Image.node.replace(imagePassthrough),
      Location.node.replace(Location.boundNode(ref)),
      Permission.node.replace(permissionLayer({ assert: () => Effect.void })),
    ],
  )
}

const callCodeMode = (registry: Tool.Interface, sessionID: Session.ID, name: string, input: unknown) =>
  executeTool(registry, {
    sessionID,
    ...toolIdentity,
    call: {
      type: "tool-call",
      id: `call-${name}`,
      name: "execute",
      input: { code: `return await tools.opencode.${name}(${JSON.stringify(input)})` },
    },
  }).pipe(
    Effect.map((result) => {
      if (result.status !== "completed") {
        throw new Error(`Tool ${name} failed: ${JSON.stringify(result.error)}`)
      }
      return JSON.parse(result.content?.[0]?.type === "text" ? result.content[0].text : "")
    }),
  )

const it = testEffect(Layer.empty)

describe("experimental.artifacts service and tools", () => {
  it.live("stays disabled and hides artifact tools when experimental.artifacts is unset", () =>
    Effect.gen(function* () {
      const dir = (yield* tmpdirScoped("opencode-artifact-off-")).path
      yield* Effect.gen(function* () {
        const artifacts = yield* Artifact.Service
        const registry = yield* Tool.Service
        expect(yield* artifacts.enabled()).toBe(false)
        expect(
          yield* artifacts
            .create({ title: "Draft", files: { "index.html": "<h1>hi</h1>" } })
            .pipe(Effect.flip),
        ).toBeInstanceOf(Artifact.DisabledError)

        yield* registerToolPlugin(ArtifactTools.Plugin, {
          event: { subscribe: () => Stream.never },
        })
        const snap = yield* registry.snapshot()
        const listed = snap.codeModeCatalog ? codeModeListings(snap.codeModeCatalog).map((item) => item.path) : []
        for (const name of toolPaths) {
          expect(listed).not.toContain(name)
        }
      }).pipe(Effect.provide(buildLayer(false, dir)))
    }),
  )

  it.live("registers custom providers, versions updates with baseVersion checks, and publishes disk folders", () =>
    Effect.gen(function* () {
      const dir = (yield* tmpdirScoped("opencode-artifact-on-")).path
      const bundleDir = path.join(dir, "bundle")
      yield* Effect.promise(async () => {
        await fs.mkdir(path.join(bundleDir, "assets"), { recursive: true })
        await fs.mkdir(path.join(bundleDir, "node_modules", "ignored"), { recursive: true })
        await fs.writeFile(path.join(bundleDir, "index.html"), "<!doctype html><script src='./assets/app.js'></script>")
        await fs.writeFile(path.join(bundleDir, "assets", "app.js"), "console.log('v1')")
        await fs.writeFile(path.join(bundleDir, "node_modules", "ignored", "x.js"), "ignored")
      })

      yield* Effect.gen(function* () {
        const artifacts = yield* Artifact.Service
        const registry = yield* Tool.Service
        const sessions = yield* Session.Service
        const session = yield* sessions.create({
          location: Location.Ref.make({ directory: AbsolutePath.make(dir) }),
        })

        const remoteStore = new Map<Artifact.ID, Record<string, Uint8Array>>()
        yield* artifacts.transform((editor) => {
          editor.add({
            id: Artifact.ProviderID.make("seer"),
            name: "Seer",
            capabilities: ["static", "markdown", "versions"],
            prompt: "Publishes versioned artifacts to Seer.",
            create: (input) =>
              Effect.sync(() => {
                remoteStore.set(input.id, { ...input.files })
                return { url: `seer://artifact/${input.id}` }
              }),
            read: (id) => Effect.sync(() => remoteStore.get(id) ?? {}),
            update: (id, input) =>
              Effect.sync(() => {
                remoteStore.set(id, { ...input.files })
                return { url: `seer://artifact/${id}` }
              }),
            delete: (id) =>
              Effect.sync(() => {
                remoteStore.delete(id)
              }),
          })
          editor.default.set(Artifact.ProviderID.make("seer"))
        })

        yield* registerToolPlugin(ArtifactTools.Plugin, {
          event: { subscribe: () => Stream.never },
        })
        const snap = yield* registry.snapshot()
        const listings = snap.codeModeCatalog ? codeModeListings(snap.codeModeCatalog) : []
        expect(listings.map((item) => item.path)).toEqual(expect.arrayContaining(toolPaths))
        const rawTools = yield* registry.list()
        const createTool = rawTools.find((item) => item.id === "opencode_artifact_create")
        expect(createTool?.description).toContain(
          "Default provider (seer [static, markdown, versions]): Publishes versioned artifacts to Seer.",
        )
        expect(createTool?.description).toContain(
          "Other active providers (pass provider to override): local [static, markdown, versions].",
        )

        const createdInfo = (yield* callCodeMode(registry, session.id, "artifact_create", {
          title: "Flamegraph report",
          dir: bundleDir,
          metadata: { card: "t851" },
        })) as Artifact.Info

        expect(createdInfo).toMatchObject({
          provider: "seer",
          sessionID: session.id,
          title: "Flamegraph report",
          entry: "index.html",
          version: 1,
          url: `seer://artifact/${createdInfo.id}`,
          metadata: { card: "t851" },
        })

        const content = (yield* callCodeMode(registry, session.id, "artifact_read", {
          id: createdInfo.id,
        })) as Artifact.Content
        expect(Object.keys(content.files).toSorted()).toEqual(["assets/app.js", "index.html"])
        expect(content.files["assets/app.js"]).toBe("console.log('v1')")

        // Per-request provider override ("local") stores independently of default ("seer")
        const localInfo = (yield* callCodeMode(registry, session.id, "artifact_create", {
          title: "Local scratch",
          files: { "index.md": "# Notes" },
          provider: "local",
        })) as Artifact.Info
        expect(localInfo.provider).toBe(Artifact.LocalProviderID)
        expect(localInfo.version).toBe(1)

        // Update increments version and checks baseVersion
        const updatedInfo = (yield* callCodeMode(registry, session.id, "artifact_update", {
          id: createdInfo.id,
          baseVersion: 1,
          files: { "index.html": "<h1>v2</h1>" },
        })) as Artifact.Info
        expect(updatedInfo.version).toBe(2)

        const stale = yield* artifacts
          .update(createdInfo.id, {
            baseVersion: 1,
            files: { "index.html": "<h1>stale</h1>" },
          })
          .pipe(Effect.flip)
        expect(stale).toBeInstanceOf(Artifact.ConflictError)

        // Reject path traversal
        const traversal = yield* artifacts
          .create({
            title: "Bad path",
            files: { "../escape.html": "nope" },
          })
          .pipe(Effect.flip)
        expect(traversal).toBeInstanceOf(Artifact.InvalidError)
      }).pipe(Effect.provide(buildLayer(true, dir)))
    }),
  )
})
