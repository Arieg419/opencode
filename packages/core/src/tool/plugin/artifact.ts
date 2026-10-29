export * as ArtifactTools from "./artifact.js"

import fs from "node:fs/promises"
import path from "node:path"
import { ToolFailure } from "@opencode/ai/effect"
import type { Context } from "@opencode/plugin/effect/plugin"
import { PositiveInt, optional } from "@opencode/schema/schema"
import { Effect, Schema, Stream } from "effect"
import { Artifact } from "../../artifact.js"
import { Config } from "../../config.js"
import { ConfigEntryObserver } from "../../config/plugin/entry-observer.js"
import { FileAccess } from "../../file-access.js"

const ExcludedDirs = new Set([".git", "node_modules", ".DS_Store"])

export const CreateToolInput = Schema.Struct({
  title: Schema.String.annotate({ description: "Human-readable artifact title." }),
  files: Schema.Record(Schema.String, Schema.String)
    .pipe(optional)
    .annotate({ description: "Inline UTF-8 file map keyed by relative path (for example index.html or index.md)." }),
  dir: Schema.String.pipe(optional).annotate({
    description:
      "Directory on disk to publish recursively (supports binary assets, images, fonts, and videos). May be combined with files.",
  }),
  entry: Schema.String.pipe(optional).annotate({
    description: "Entry file path relative to the artifact root. Defaults to index.html, index.md, or the sole file.",
  }),
  provider: Schema.String.pipe(optional).annotate({
    description: "Artifact provider ID to override the default provider for this request.",
  }),
  metadata: Schema.Record(Schema.String, Schema.String)
    .pipe(optional)
    .annotate({ description: "Optional key-value metadata passed to the artifact provider." }),
})

export const ReadToolInput = Schema.Struct({
  id: Artifact.ID.annotate({ description: "Artifact ID (art_...) to read." }),
})

export const UpdateToolInput = Schema.Struct({
  id: Artifact.ID.annotate({ description: "Artifact ID (art_...) to update." }),
  title: Schema.String.pipe(optional).annotate({ description: "Updated title, or omit to keep the current title." }),
  files: Schema.Record(Schema.String, Schema.String)
    .pipe(optional)
    .annotate({ description: "Replacement UTF-8 file map keyed by relative path." }),
  dir: Schema.String.pipe(optional).annotate({
    description: "Replacement directory on disk to publish recursively. May be combined with files.",
  }),
  entry: Schema.String.pipe(optional).annotate({ description: "Updated entry file path." }),
  baseVersion: PositiveInt.pipe(optional).annotate({
    description: "Expected current version for optimistic concurrency; fails on mismatch.",
  }),
  metadata: Schema.Record(Schema.String, Schema.String)
    .pipe(optional)
    .annotate({ description: "Optional metadata merged onto the artifact." }),
})

export const DeleteToolInput = Schema.Struct({
  id: Artifact.ID.annotate({ description: "Artifact ID (art_...) to delete." }),
})

export const ListToolInput = Schema.Struct({
  sessionOnly: Schema.Boolean.pipe(optional).annotate({
    description: "When true (default), list only artifacts linked to the current session.",
  }),
  provider: Schema.String.pipe(optional).annotate({ description: "Filter by provider ID." }),
})

const DeleteToolOutput = Schema.Struct({
  id: Artifact.ID,
  deleted: Schema.Boolean,
})

const ListToolOutput = Schema.Struct({
  artifacts: Schema.Array(Artifact.Info),
})

const encoder = new TextEncoder()

export const Plugin = {
  id: "opencode.tool.artifact",
  effect: Effect.fn("ArtifactTools.Plugin")(function* (ctx: Context) {
    const config = yield* Config.Service
    const artifacts = yield* Artifact.Service
    const access = yield* FileAccess.Service

    const promptSnapshot = { text: yield* renderCreateDescription(artifacts) }
    const refreshTools = Effect.gen(function* () {
      promptSnapshot.text = yield* renderCreateDescription(artifacts)
      yield* ctx.tool.reload()
    })

    const loaded = yield* ConfigEntryObserver.observe(config, ctx.event, refreshTools)
    yield* ctx.event.subscribe({ types: [Artifact.Event.Updated.type] }).pipe(
      Stream.runForEach(() => refreshTools),
      Effect.forkScoped({ startImmediately: true }),
    )

    const enabled = () => Config.latest(loaded.entries, "experimental")?.artifacts === true
    const failure = (error: { readonly message: string }) => new ToolFailure({ message: error.message, error })

    const collectInputFiles = (
      input: { readonly dir?: string; readonly files?: Readonly<Record<string, string>> },
      toolContext: Parameters<Parameters<typeof ctx.tool.transform>[0]>[0] extends {
        add(info: { execute: (input: never, context: infer C) => unknown }): void
      }
        ? C
        : never,
    ) =>
      Effect.gen(function* () {
        const merged: Record<string, Uint8Array> = {}
        if (input.dir !== undefined) {
          const target = yield* access.authorizeRead(input.dir, toolContext)
          const diskFiles = yield* readDirectoryBytes(target.absolute)
          Object.assign(merged, diskFiles)
        }
        if (input.files !== undefined) {
          for (const [relPath, text] of Object.entries(input.files)) {
            merged[relPath] = encoder.encode(text)
          }
        }
        return merged
      })

    yield* ctx.tool
      .transform((editor) => {
        if (!enabled()) return

        editor.add({
          name: "artifact_create",
          description: promptSnapshot.text,
          input: CreateToolInput,
          output: Artifact.Info,
          options: { namespace: "opencode", codemode: true },
          execute: (input, context) =>
            Effect.gen(function* () {
              const files = yield* collectInputFiles(input, context)
              const info = yield* artifacts.createBytes({
                title: input.title,
                files,
                entry: input.entry,
                provider: input.provider === undefined ? undefined : Artifact.ProviderID.make(input.provider),
                sessionID: context.sessionID,
                metadata: input.metadata,
              })
              return {
                output: info,
                content: `Published ${info.title} (${info.id} v${info.version} via ${info.provider}): ${info.url}`,
              }
            }).pipe(Effect.mapError(failure)),
        })

        editor.add({
          name: "artifact_read",
          description: "Read an artifact's metadata and file contents so you can inspect or update it.",
          input: ReadToolInput,
          output: Artifact.Content,
          options: { namespace: "opencode", codemode: true },
          execute: (input) =>
            artifacts.get(input.id).pipe(
              Effect.map((content) => ({
                output: content,
                content: `${content.info.title} (${content.info.id} v${content.info.version} · ${Object.keys(content.files).length} files)`,
              })),
              Effect.mapError(failure),
            ),
        })

        editor.add({
          name: "artifact_update",
          description:
            "Publish a new version of an existing artifact at the same ID and URL. Pass baseVersion to guard against overwriting concurrent updates.",
          input: UpdateToolInput,
          output: Artifact.Info,
          options: { namespace: "opencode", codemode: true },
          execute: (input, context) =>
            Effect.gen(function* () {
              const files = yield* collectInputFiles(input, context)
              const info = yield* artifacts.updateBytes(input.id, {
                title: input.title,
                files,
                entry: input.entry,
                baseVersion: input.baseVersion,
                sessionID: context.sessionID,
                metadata: input.metadata,
              })
              return {
                output: info,
                content: `Updated ${info.title} (${info.id} v${info.version}): ${info.url}`,
              }
            }).pipe(Effect.mapError(failure)),
        })

        editor.add({
          name: "artifact_delete",
          description: "Delete an artifact and remove its stored copy.",
          input: DeleteToolInput,
          output: DeleteToolOutput,
          options: { namespace: "opencode", codemode: true },
          execute: (input) =>
            artifacts.remove(input.id).pipe(
              Effect.map(() => ({
                output: { id: input.id, deleted: true },
                content: `Deleted ${input.id}`,
              })),
              Effect.mapError(failure),
            ),
        })

        editor.add({
          name: "artifact_list",
          description: "List published artifacts, defaulting to the current session's artifacts.",
          input: ListToolInput,
          output: ListToolOutput,
          options: { namespace: "opencode", codemode: true },
          execute: (input, context) =>
            artifacts
              .list({
                sessionID: input.sessionOnly === false ? undefined : context.sessionID,
                provider: input.provider === undefined ? undefined : Artifact.ProviderID.make(input.provider),
              })
              .pipe(
                Effect.map((list) => ({
                  output: { artifacts: [...list] },
                  content:
                    list.length === 0
                      ? "No artifacts."
                      : list.map((item) => `${item.id} v${item.version} [${item.provider}] ${item.title} (${item.url})`).join("\n"),
                })),
                Effect.mapError(failure),
              ),
        })
      })
      .pipe(Effect.orDie)
  }),
}

function renderCreateDescription(artifacts: Artifact.Interface) {
  return artifacts.prompts().pipe(
    Effect.map(({ defaultProvider, others }) => {
      const lines = [
        "Publish a new artifact (HTML, Markdown, or multi-file folder) to the default artifact provider or a named override.",
        `Default provider (${defaultProvider.id} [${defaultProvider.capabilities.join(", ")}]): ${defaultProvider.prompt}`,
      ]
      if (others.length > 0) {
        lines.push(
          `Other active providers (pass provider to override): ${others.map((item) => `${item.id} [${item.capabilities.join(", ")}]`).join(", ")}.`,
        )
      }
      return lines.join("\n")
    }),
  )
}

function readDirectoryBytes(root: string) {
  return Effect.tryPromise({
    try: async () => {
      const stat = await fs.stat(root)
      if (!stat.isDirectory()) throw new Error(`Not a directory: ${root}`)
      const out: Record<string, Uint8Array> = {}
      const walk = async (dir: string, prefix: string): Promise<void> => {
        const entries = await fs.readdir(dir, { withFileTypes: true })
        await Promise.all(
          entries.map(async (entry) => {
            if (ExcludedDirs.has(entry.name) || entry.name.startsWith(".artifact")) return
            const rel = prefix ? `${prefix}/${entry.name}` : entry.name
            const full = path.join(dir, entry.name)
            if (entry.isDirectory()) return walk(full, rel)
            if (entry.isFile()) out[rel] = new Uint8Array(await fs.readFile(full))
          }),
        )
      }
      await walk(root, "")
      return out
    },
    catch: (cause) =>
      new Artifact.InvalidError({
        reason: cause instanceof Error ? cause.message : `Unable to read artifact directory: ${root}`,
        field: "dir",
      }),
  })
}
