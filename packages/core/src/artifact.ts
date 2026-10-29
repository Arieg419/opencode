export * as Artifact from "./artifact.js"

import fs from "node:fs/promises"
import path from "node:path"
import { lookup } from "mime-types"
import {
  Capability,
  Content,
  CreateInput,
  Event,
  ID,
  Info,
  Provider,
  ProviderID,
  UpdateInput,
} from "@opencode/schema/artifact"
import type { SessionID } from "@opencode/schema/session-id"
import { Global } from "@opencode/util/global"
import { makeLocationNode } from "@opencode/util/effect/app-node"
import { Clock, Context, Effect, Layer, Option, Schema } from "effect"
import { Bus } from "./bus.js"
import { Config } from "./config.js"
import { KV } from "./kv.js"
import { Mime } from "./mime.js"
import { Session } from "./session.js"
import { State } from "./state.js"

export { Capability, Content, CreateInput, Event, ID, Info, Provider, ProviderID, UpdateInput }

export const LocalProviderID = ProviderID.make("local")
const ItemPrefix = "artifact:item:"
const DefaultProviderKey = "artifact:provider"

export interface ProviderCreateInput {
  readonly id: ID
  readonly title: string
  readonly entry: string
  readonly version: number
  readonly files: Readonly<Record<string, Uint8Array>>
  readonly sessionID?: SessionID
  readonly metadata?: Readonly<Record<string, string>>
}

export interface ProviderUpdateInput {
  readonly title: string
  readonly entry: string
  readonly version: number
  readonly files: Readonly<Record<string, Uint8Array>>
  readonly sessionID?: SessionID
  readonly metadata?: Readonly<Record<string, string>>
}

export interface ProviderResult {
  readonly url?: string
}

export interface ProviderImplementation {
  readonly id: ProviderID
  readonly name: string
  readonly capabilities: readonly Capability[]
  readonly prompt: string
  readonly create: (input: ProviderCreateInput) => Effect.Effect<ProviderResult, unknown>
  readonly read: (id: ID) => Effect.Effect<Readonly<Record<string, Uint8Array>>, unknown>
  readonly update: (id: ID, input: ProviderUpdateInput) => Effect.Effect<ProviderResult, unknown>
  readonly delete: (id: ID) => Effect.Effect<void, unknown>
}

export interface CreateBytesInput {
  readonly title: string
  readonly files: Readonly<Record<string, Uint8Array>>
  readonly entry?: string
  readonly provider?: ProviderID
  readonly sessionID?: SessionID
  readonly metadata?: Readonly<Record<string, string>>
}

export interface UpdateBytesInput {
  readonly title?: string
  readonly files: Readonly<Record<string, Uint8Array>>
  readonly entry?: string
  readonly baseVersion?: number
  readonly sessionID?: SessionID
  readonly metadata?: Readonly<Record<string, string>>
}

export interface LocalFile {
  readonly bytes: Uint8Array
  readonly mime: string
  readonly entry: string
  readonly version: number
}

export interface ProviderPrompts {
  readonly defaultProvider: {
    readonly id: ProviderID
    readonly name: string
    readonly capabilities: readonly Capability[]
    readonly prompt: string
  }
  readonly others: readonly {
    readonly id: ProviderID
    readonly name: string
    readonly capabilities: readonly Capability[]
  }[]
}

export class DisabledError extends Schema.TaggedError<DisabledError>()("Artifact.DisabledError", {}) {
  override get message() {
    return "Artifacts are disabled; set experimental.artifacts in config"
  }
}

export class NotFoundError extends Schema.TaggedError<NotFoundError>()("Artifact.NotFoundError", {
  artifactID: ID,
}) {
  override get message() {
    return `Artifact not found: ${this.artifactID}`
  }
}

export class FileMissingError extends Schema.TaggedError<FileMissingError>()("Artifact.FileMissingError", {
  artifactID: ID,
  path: Schema.String,
}) {
  override get message() {
    return `Artifact file not found: ${this.path}`
  }
}

export class ProviderNotFoundError extends Schema.TaggedError<ProviderNotFoundError>()(
  "Artifact.ProviderNotFoundError",
  {
    providerID: ProviderID,
  },
) {
  override get message() {
    return `Artifact provider not found: ${this.providerID}`
  }
}

export class InvalidError extends Schema.TaggedError<InvalidError>()("Artifact.InvalidError", {
  reason: Schema.String,
  field: Schema.optional(Schema.String),
}) {
  override get message() {
    return this.reason
  }
}

export class ConflictError extends Schema.TaggedError<ConflictError>()("Artifact.ConflictError", {
  artifactID: ID,
  baseVersion: Schema.Number,
  currentVersion: Schema.Number,
}) {
  override get message() {
    return `Artifact ${this.artifactID} is at version ${this.currentVersion} (expected baseVersion ${this.baseVersion})`
  }
}

export class ProviderError extends Schema.TaggedError<ProviderError>()("Artifact.ProviderError", {
  providerID: ProviderID,
  cause: Schema.Defect(),
}) {
  override get message() {
    return this.cause instanceof Error && this.cause.message
      ? `Artifact provider ${this.providerID} failed: ${this.cause.message}`
      : `Artifact provider ${this.providerID} failed`
  }
}

export type Error =
  | DisabledError
  | NotFoundError
  | FileMissingError
  | ProviderNotFoundError
  | InvalidError
  | ConflictError
  | ProviderError
  | Session.NotFoundError

export type Editor = {
  add: (provider: ProviderImplementation) => void
  remove: (id: ProviderID) => void
  default: {
    get: () => ProviderID | undefined
    set: (providerID: ProviderID | undefined) => void
  }
}

type Data = {
  readonly providers: Map<ProviderID, ProviderImplementation>
  selection?: ProviderID
}

export interface Interface extends State.Transformable<Editor> {
  readonly enabled: () => Effect.Effect<boolean>
  readonly providers: () => Effect.Effect<readonly Provider[], DisabledError>
  readonly prompts: () => Effect.Effect<ProviderPrompts>
  readonly list: (filter?: {
    readonly sessionID?: SessionID
    readonly provider?: ProviderID
  }) => Effect.Effect<readonly Info[], DisabledError | Session.NotFoundError>
  readonly create: (input: CreateInput) => Effect.Effect<Info, Error>
  readonly createBytes: (input: CreateBytesInput) => Effect.Effect<Info, Error>
  readonly get: (id: ID) => Effect.Effect<Content, DisabledError | NotFoundError | ProviderNotFoundError | ProviderError>
  readonly info: (id: ID) => Effect.Effect<Info, DisabledError | NotFoundError>
  readonly update: (id: ID, input: UpdateInput) => Effect.Effect<Info, Error>
  readonly updateBytes: (id: ID, input: UpdateBytesInput) => Effect.Effect<Info, Error>
  readonly remove: (id: ID) => Effect.Effect<void, DisabledError | NotFoundError | ProviderError>
  readonly readLocalFile: (
    id: ID,
    options?: { readonly path?: string; readonly version?: number; readonly raw?: boolean },
  ) => Effect.Effect<LocalFile, DisabledError | NotFoundError | FileMissingError | InvalidError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Artifact") {}

const decodeStoredInfo = Schema.decodeUnknownOption(Info)
const encodeStoredInfo = Schema.encodeSync(Info)
const encoder = new TextEncoder()

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const bus = yield* Bus.Service
    const config = yield* Config.Service
    const global = yield* Global.Service
    const kv = yield* KV.Service
    const sessions = yield* Session.Service

    const rootDir = path.join(global.data, "artifacts")
    const versionDir = (id: ID, version: number) => path.join(rootDir, id, `v${version}`)

    const localProvider: ProviderImplementation = {
      id: LocalProviderID,
      name: "Local",
      capabilities: ["static", "markdown", "versions"],
      prompt:
        "Artifacts are stored on disk and served as HTML by this OpenCode server. Only people who can reach this server can open them.",
      create: (input) =>
        writeVersionTree(versionDir(input.id, input.version), input.files).pipe(
          Effect.as({ url: `/api/experimental/artifacts/${encodeURIComponent(input.id)}/view/` }),
        ),
      read: (id) =>
        Effect.gen(function* () {
          const current = yield* loadInfo(kv, id)
          if (!current) return {}
          return yield* readVersionTree(versionDir(id, current.version))
        }),
      update: (id, input) =>
        writeVersionTree(versionDir(id, input.version), input.files).pipe(
          Effect.as({ url: `/api/experimental/artifacts/${encodeURIComponent(id)}/view/` }),
        ),
      delete: (id) =>
        Effect.promise(() => fs.rm(path.join(rootDir, id), { recursive: true, force: true })),
    }

    const snapshotProviders = (data: Data) =>
      Array.from(data.providers.values(), (provider) => ({
        id: provider.id,
        name: provider.name,
        capabilities: provider.capabilities,
        selection: data.selection,
      })).toSorted((a, b) => a.id.localeCompare(b.id))

    const state = State.create<Data, Editor>({
      initial: () => ({
        providers: new Map([[LocalProviderID, localProvider]]),
      }),
      editor: (draft) => ({
        add: (provider) => {
          draft.providers.set(provider.id, provider)
        },
        remove: (id) => {
          if (id === LocalProviderID) return
          draft.providers.delete(id)
          if (draft.selection === id) draft.selection = undefined
        },
        default: {
          get: () => draft.selection,
          set: (providerID) => {
            draft.selection = providerID
          },
        },
      }),
      notify: State.changes(snapshotProviders, () => bus.publish(Event.Updated, {}).pipe(Effect.asVoid)),
    })

    const isEnabled = Effect.fn("Artifact.enabled")(function* () {
      const entries = yield* config.entries()
      return Config.latest(entries, "experimental")?.artifacts === true
    })

    const requireEnabled = () =>
      isEnabled().pipe(Effect.flatMap((on) => (on ? Effect.void : Effect.fail(new DisabledError()))))

    const activeDefaultID = Effect.fn("Artifact.activeDefaultID")(function* () {
      const data = state.get()
      if (data.selection && data.providers.has(data.selection)) return data.selection
      const stored = yield* kv.get(DefaultProviderKey)
      const decoded = Schema.decodeUnknownOption(ProviderID)(stored)
      if (Option.isSome(decoded) && data.providers.has(decoded.value)) return decoded.value
      return LocalProviderID
    })

    const resolveProvider = Effect.fn("Artifact.resolveProvider")(function* (requested?: ProviderID) {
      const data = state.get()
      const target = requested ?? (yield* activeDefaultID())
      const found = data.providers.get(target)
      if (!found) return yield* new ProviderNotFoundError({ providerID: target })
      return found
    })

    const checkSession = (sessionID?: SessionID) =>
      sessionID === undefined ? Effect.void : sessions.get(sessionID).pipe(Effect.asVoid)

    const createBytes = Effect.fn("Artifact.createBytes")(function* (input: CreateBytesInput) {
      yield* requireEnabled()
      yield* checkSession(input.sessionID)
      const title = input.title.trim()
      if (!title) return yield* new InvalidError({ reason: "Artifact title must not be empty", field: "title" })
      const files = yield* normalizeFiles(input.files)
      const entry = yield* resolveEntry(files, input.entry)
      const provider = yield* resolveProvider(input.provider)
      const id = ID.create()
      const version = 1
      const now = yield* Clock.currentTimeMillis
      const created = yield* provider
        .create({
          id,
          title,
          entry,
          version,
          files,
          sessionID: input.sessionID,
          metadata: input.metadata,
        })
        .pipe(Effect.mapError((cause) => new ProviderError({ providerID: provider.id, cause })))
      const info: Info = Info.make({
        id,
        provider: provider.id,
        ...(input.sessionID === undefined ? {} : { sessionID: input.sessionID }),
        title,
        entry,
        url: created.url ?? `/api/experimental/artifacts/${encodeURIComponent(id)}/view/`,
        version,
        ...(input.metadata === undefined ? {} : { metadata: input.metadata }),
        time: { created: now, updated: now },
      })
      yield* kv.set(ItemPrefix + id, encodeStoredInfo(info))
      yield* bus.publish(Event.Updated, {})
      return info
    })

    const updateBytes = Effect.fn("Artifact.updateBytes")(function* (id: ID, input: UpdateBytesInput) {
      yield* requireEnabled()
      yield* checkSession(input.sessionID)
      const existing = yield* loadInfo(kv, id)
      if (!existing) return yield* new NotFoundError({ artifactID: id })
      if (input.baseVersion !== undefined && input.baseVersion !== existing.version) {
        return yield* new ConflictError({
          artifactID: id,
          baseVersion: input.baseVersion,
          currentVersion: existing.version,
        })
      }
      const title = input.title === undefined ? existing.title : input.title.trim()
      if (!title) return yield* new InvalidError({ reason: "Artifact title must not be empty", field: "title" })
      const files = yield* normalizeFiles(input.files)
      const entry = yield* resolveEntry(files, input.entry ?? (files[existing.entry] ? existing.entry : undefined))
      const provider = yield* resolveProvider(existing.provider)
      const version = existing.version + 1
      const now = yield* Clock.currentTimeMillis
      const sessionID = input.sessionID ?? existing.sessionID
      const metadata = input.metadata === undefined ? existing.metadata : { ...existing.metadata, ...input.metadata }
      const updated = yield* provider
        .update(id, {
          title,
          entry,
          version,
          files,
          sessionID,
          metadata,
        })
        .pipe(Effect.mapError((cause) => new ProviderError({ providerID: provider.id, cause })))
      const info: Info = Info.make({
        id,
        provider: existing.provider,
        ...(sessionID === undefined ? {} : { sessionID }),
        title,
        entry,
        url: updated.url ?? existing.url,
        version,
        ...(metadata === undefined ? {} : { metadata }),
        time: { created: existing.time.created, updated: now },
      })
      yield* kv.set(ItemPrefix + id, encodeStoredInfo(info))
      yield* bus.publish(Event.Updated, {})
      return info
    })

    return Service.of({
      transform: state.transform,
      reload: state.reload,
      enabled: isEnabled,
      providers: Effect.fn("Artifact.providers")(function* () {
        yield* requireEnabled()
        const defaultID = yield* activeDefaultID()
        return Array.from(state.get().providers.values(), (provider) =>
          Provider.make({
            id: provider.id,
            name: provider.name,
            capabilities: [...provider.capabilities],
            default: provider.id === defaultID,
          }),
        ).toSorted((a, b) => Number(b.default) - Number(a.default) || a.name.localeCompare(b.name))
      }),
      prompts: Effect.fn("Artifact.prompts")(function* () {
        const defaultID = yield* activeDefaultID()
        const all = Array.from(state.get().providers.values())
        const defaultProvider = state.get().providers.get(defaultID) ?? localProvider
        const others = all
          .filter((item) => item.id !== defaultProvider.id)
          .toSorted((a, b) => a.name.localeCompare(b.name))
          .map((item) => ({ id: item.id, name: item.name, capabilities: item.capabilities }))
        return {
          defaultProvider: {
            id: defaultProvider.id,
            name: defaultProvider.name,
            capabilities: defaultProvider.capabilities,
            prompt: defaultProvider.prompt,
          },
          others,
        }
      }),
      list: Effect.fn("Artifact.list")(function* (filter) {
        yield* requireEnabled()
        if (filter?.sessionID !== undefined) yield* checkSession(filter.sessionID)
        const items = yield* scanAllInfos(kv)
        return items
          .filter(
            (item) =>
              (filter?.sessionID === undefined || item.sessionID === filter.sessionID) &&
              (filter?.provider === undefined || item.provider === filter.provider),
          )
          .toSorted((a, b) => b.time.updated - a.time.updated || b.id.localeCompare(a.id))
      }),
      create: (input) =>
        createBytes({
          title: input.title,
          files: encodeStringFiles(input.files),
          entry: input.entry,
          provider: input.provider,
          sessionID: input.sessionID,
          metadata: input.metadata,
        }),
      createBytes,
      info: Effect.fn("Artifact.info")(function* (id) {
        yield* requireEnabled()
        const existing = yield* loadInfo(kv, id)
        if (!existing) return yield* new NotFoundError({ artifactID: id })
        return existing
      }),
      get: Effect.fn("Artifact.get")(function* (id) {
        yield* requireEnabled()
        const existing = yield* loadInfo(kv, id)
        if (!existing) return yield* new NotFoundError({ artifactID: id })
        const provider = yield* resolveProvider(existing.provider)
        const raw = yield* provider
          .read(id)
          .pipe(Effect.mapError((cause) => new ProviderError({ providerID: provider.id, cause })))
        return Content.make({
          info: existing,
          files: decodeReadFiles(raw),
        })
      }),
      update: (id, input) =>
        updateBytes(id, {
          title: input.title,
          files: encodeStringFiles(input.files),
          entry: input.entry,
          baseVersion: input.baseVersion,
          sessionID: input.sessionID,
          metadata: input.metadata,
        }),
      updateBytes,
      remove: Effect.fn("Artifact.remove")(function* (id) {
        yield* requireEnabled()
        const existing = yield* loadInfo(kv, id)
        if (!existing) return yield* new NotFoundError({ artifactID: id })
        const provider = state.get().providers.get(existing.provider)
        if (provider) {
          yield* provider
            .delete(id)
            .pipe(Effect.mapError((cause) => new ProviderError({ providerID: provider.id, cause })))
        }
        yield* kv.remove(ItemPrefix + id)
        yield* bus.publish(Event.Updated, {})
      }),
      readLocalFile: Effect.fn("Artifact.readLocalFile")(function* (id, options) {
        yield* requireEnabled()
        const existing = yield* loadInfo(kv, id)
        if (!existing) return yield* new NotFoundError({ artifactID: id })
        if (existing.provider !== LocalProviderID) {
          return yield* new InvalidError({
            reason: `Artifact ${id} is hosted by provider "${existing.provider}", not the local server`,
            field: "artifactID",
          })
        }
        const version = options?.version ?? existing.version
        if (version < 1 || version > existing.version) {
          return yield* new FileMissingError({ artifactID: id, path: `v${version}` })
        }
        const requested = options?.path?.trim() ? yield* normalizePath(options.path) : existing.entry
        const base = versionDir(id, version)
        const target = path.join(base, ...requested.split("/"))
        const exists = yield* Effect.promise(() =>
          fs
            .stat(target)
            .then((stat) => stat.isFile())
            .catch(() => false),
        )
        if (!exists) return yield* new FileMissingError({ artifactID: id, path: requested })
        const bytes = yield* Effect.promise(() => fs.readFile(target).then((buf) => new Uint8Array(buf)))
        if (!options?.raw && requested === existing.entry && requested.toLowerCase().endsWith(".md")) {
          const markdown = new TextDecoder().decode(bytes)
          return {
            bytes: encoder.encode(renderMarkdownShell(existing.title, markdown)),
            mime: "text/html; charset=utf-8",
            entry: existing.entry,
            version,
          }
        }
        return {
          bytes,
          mime: detectFileMime(requested, bytes),
          entry: existing.entry,
          version,
        }
      }),
    })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [Bus.node, Config.node, Global.node, KV.node, Session.node],
})

function encodeStringFiles(files: Readonly<Record<string, string>>) {
  return Object.fromEntries(Object.entries(files).map(([filePath, content]) => [filePath, encoder.encode(content)]))
}

function decodeReadFiles(files: Readonly<Record<string, Uint8Array>>) {
  const decoder = new TextDecoder("utf-8", { fatal: true })
  return Object.fromEntries(
    Object.entries(files).map(([filePath, bytes]) => {
      const mime = detectFileMime(filePath, bytes)
      if (!Mime.isTextual(mime.split(";")[0]?.trim() ?? mime)) {
        return [filePath, `data:${mime};base64,${Buffer.from(bytes).toString("base64")}`]
      }
      const text = decodeUtf8(decoder, bytes)
      return [
        filePath,
        text === undefined ? `data:application/octet-stream;base64,${Buffer.from(bytes).toString("base64")}` : text,
      ]
    }),
  )
}

function decodeUtf8(decoder: TextDecoder, bytes: Uint8Array) {
  try {
    return decoder.decode(bytes)
  } catch {
    return undefined
  }
}

function detectFileMime(filePath: string, bytes: Uint8Array) {
  const byName = lookup(filePath)
  if (typeof byName === "string") {
    return byName.startsWith("text/") || byName === "application/javascript" || byName === "application/json"
      ? `${byName}; charset=utf-8`
      : byName
  }
  return Mime.detect(bytes)
}

function normalizePath(raw: string): Effect.Effect<string, InvalidError> {
  const trimmed = raw.replaceAll("\\", "/").replace(/^(?:\.\/)+/u, "").replace(/^\/+/u, "")
  if (!trimmed || trimmed.includes("\0")) {
    return Effect.fail(new InvalidError({ reason: `Invalid artifact file path: ${JSON.stringify(raw)}`, field: "files" }))
  }
  const segments = trimmed.split("/")
  if (segments.some((part) => !part || part === "." || part === "..")) {
    return Effect.fail(
      new InvalidError({ reason: `Artifact file path must stay relative: ${JSON.stringify(raw)}`, field: "files" }),
    )
  }
  return Effect.succeed(segments.join("/"))
}

function normalizeFiles(files: Readonly<Record<string, Uint8Array>>) {
  return Effect.gen(function* () {
    const entries = Object.entries(files)
    if (entries.length === 0) {
      return yield* new InvalidError({ reason: "Artifact must include at least one file", field: "files" })
    }
    const normalized = new Map<string, Uint8Array>()
    for (const [rawPath, content] of entries) {
      const clean = yield* normalizePath(rawPath)
      normalized.set(clean, content)
    }
    return Object.fromEntries(normalized)
  })
}

function resolveEntry(files: Readonly<Record<string, Uint8Array>>, requested?: string) {
  if (requested !== undefined) {
    return normalizePath(requested).pipe(
      Effect.flatMap((clean) =>
        files[clean] !== undefined
          ? Effect.succeed(clean)
          : Effect.fail(new InvalidError({ reason: `Entry file not found in files: ${clean}`, field: "entry" })),
      ),
    )
  }
  if (files["index.html"] !== undefined) return Effect.succeed("index.html")
  if (files["index.md"] !== undefined) return Effect.succeed("index.md")
  const keys = Object.keys(files)
  if (keys.length === 1 && keys[0] !== undefined) return Effect.succeed(keys[0])
  return Effect.fail(
    new InvalidError({
      reason: 'Include "index.html" or "index.md", or specify "entry" when publishing multiple files',
      field: "entry",
    }),
  )
}

function loadInfo(kv: KV.Interface, id: ID) {
  return kv.get(ItemPrefix + id).pipe(
    Effect.map((stored) => (stored === undefined ? undefined : Option.getOrUndefined(decodeStoredInfo(stored)))),
  )
}

function scanAllInfos(kv: KV.Interface) {
  return Effect.gen(function* () {
    const results: Info[] = []
    let after: string | undefined
    while (true) {
      const page = yield* kv.scan({ prefix: ItemPrefix, after, limit: 500 })
      results.push(
        ...page.entries.flatMap((entry) => {
          const decoded = decodeStoredInfo(entry.value)
          return Option.isSome(decoded) ? [decoded.value] : []
        }),
      )
      if (!page.next) return results
      after = page.next
    }
  })
}

function writeVersionTree(baseDir: string, files: Readonly<Record<string, Uint8Array>>) {
  return Effect.promise(async () => {
    await fs.mkdir(baseDir, { recursive: true })
    await Promise.all(
      Object.entries(files).map(async ([rel, bytes]) => {
        const dest = path.join(baseDir, ...rel.split("/"))
        await fs.mkdir(path.dirname(dest), { recursive: true })
        await fs.writeFile(dest, bytes)
      }),
    )
  })
}

function readVersionTree(baseDir: string) {
  return Effect.promise(async () => {
    const out: Record<string, Uint8Array> = {}
    const walk = async (current: string, prefix: string): Promise<void> => {
      const entries = await fs.readdir(current, { withFileTypes: true }).catch(() => [])
      await Promise.all(
        entries.map(async (entry) => {
          const rel = prefix ? `${prefix}/${entry.name}` : entry.name
          const full = path.join(current, entry.name)
          if (entry.isDirectory()) return walk(full, rel)
          if (entry.isFile()) out[rel] = new Uint8Array(await fs.readFile(full))
        }),
      )
    }
    await walk(baseDir, "")
    return out
  })
}

function escapeHtml(text: string) {
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
}

function renderMarkdownShell(title: string, markdown: string) {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark light">
<title>${escapeHtml(title)}</title>
<style>
  :root { color-scheme: dark light; }
  body { margin: 0; padding: 48px 24px 80px; font: 15px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; max-width: 760px; margin-inline: auto; }
  h1 { font-size: 24px; line-height: 1.25; margin: 0 0 20px; }
  pre { white-space: pre-wrap; word-break: break-word; font: 13.5px/1.6 ui-monospace, "SF Mono", Menlo, monospace; margin: 0; }
</style>
</head>
<body>
<h1>${escapeHtml(title)}</h1>
<pre>${escapeHtml(markdown)}</pre>
</body>
</html>`
}
