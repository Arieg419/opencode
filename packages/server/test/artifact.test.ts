import fs from "node:fs/promises"
import path from "node:path"
import { expect } from "bun:test"
import { OpenCode } from "@opencode/client"
import { Effect } from "effect"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const client = (handler: (request: Request) => Promise<Response>) =>
  OpenCode.make({
    baseUrl: "http://opencode.local",
    fetch: Object.assign((input: string | URL | Request, init?: RequestInit) => handler(new Request(input, init)), {
      preconnect: fetch.preconnect,
    }),
  })

it.live("manages local and plugin-backed artifacts through /api/experimental/artifacts when enabled", () =>
  Effect.gen(function* () {
    const dir = (yield* tmpdirScoped("opencode-artifact-api-")).path
    const global = path.join(dir, "global")
    const enabled = path.join(dir, "enabled")
    const disabled = path.join(dir, "disabled")
    const pluginDir = path.join(dir, "custom-artifact-plugin")

    yield* Effect.promise(async () => {
      await Promise.all([global, enabled, disabled, pluginDir].map((item) => fs.mkdir(item, { recursive: true })))
      await fs.writeFile(
        path.join(pluginDir, "package.json"),
        JSON.stringify({ name: "custom-artifact-plugin", type: "module", exports: "./index.js" }),
      )
      await fs.writeFile(
        path.join(pluginDir, "index.js"),
        `
const store = new Map()
const decoder = new TextDecoder()
const encoder = new TextEncoder()
export default {
  id: "test-r2-artifacts",
  async setup(ctx) {
    await ctx.experimental.artifact.transform((editor) => {
      editor.add({
        id: "r2",
        name: "R2 Bucket",
        capabilities: ["static", "versions"],
        prompt: "Publishes static files to R2.",
        async create(input) {
          store.set(input.id, Object.fromEntries(Object.entries(input.files).map(([k, v]) => [k, decoder.decode(v)])))
          return { url: "https://artifacts.example.test/" + input.id + "/" }
        },
        async read(id) {
          const item = store.get(id) ?? {}
          return Object.fromEntries(Object.entries(item).map(([k, v]) => [k, encoder.encode(v)]))
        },
        async update(id, input) {
          store.set(id, Object.fromEntries(Object.entries(input.files).map(([k, v]) => [k, decoder.decode(v)])))
          return { url: "https://artifacts.example.test/" + id + "/" }
        },
        async delete(id) {
          store.delete(id)
        },
      })
    })
  },
}
`,
      )
      await fs.writeFile(
        path.join(enabled, "opencode.json"),
        JSON.stringify({
          plugins: [{ package: pluginDir }],
          experimental: { artifacts: true },
        }),
      )
    })

    const handler = yield* ServerFetch.make({
      app: { version: "test" },
      database: { path: path.join(dir, "opencode.db") },
      config: { directory: global, project: true, claude: false },
      models: { fetch: false },
      fs: { filewatcher: false },
    })
    const api = client(handler)

    yield* Effect.promise(async () => {
      // Disabled location returns ForbiddenError
      await expect(
        api.experimental.artifacts.list({ location: { directory: disabled } }),
      ).rejects.toMatchObject({ _tag: "ForbiddenError" })

      // Providers list includes built-in local (default) and plugin-registered r2
      const providers = await api.experimental.artifacts.providers({ location: { directory: enabled } })
      expect(providers.data).toEqual([
        { id: "local", name: "Local", capabilities: ["static", "markdown", "versions"], default: true },
        { id: "r2", name: "R2 Bucket", capabilities: ["static", "versions"], default: false },
      ])

      const session = await api.session.create({ location: { directory: enabled } })

      // Create local multi-file artifact linked to session
      const created = await api.experimental.artifacts.create({
        location: { directory: enabled },
        sessionID: session.id,
        title: "Wait stats",
        files: {
          "index.html": "<!doctype html><h1>v1</h1><script src='./assets/app.js'></script>",
          "assets/app.js": "window.VERSION = 1",
        },
      })
      expect(created.data).toMatchObject({
        provider: "local",
        sessionID: session.id,
        title: "Wait stats",
        entry: "index.html",
        version: 1,
        url: `/api/experimental/artifacts/${encodeURIComponent(created.data.id)}/view/`,
      })

      // Redirects /view -> /view/ with 308 so relative "./assets/*" resolves
      const redirect = await handler(
        new Request(`http://opencode.local/api/experimental/artifacts/${created.data.id}/view`),
      )
      expect(redirect.status).toBe(308)
      expect(redirect.headers.get("location")).toBe(`/api/experimental/artifacts/${created.data.id}/view/`)

      // Serves entry HTML and relative JS without requiring location query when linked to session
      const v1Html = await handler(
        new Request(`http://opencode.local/api/experimental/artifacts/${created.data.id}/view/`),
      )
      expect(v1Html.status).toBe(200)
      expect(v1Html.headers.get("content-type")).toContain("text/html")
      expect(await v1Html.text()).toContain("<h1>v1</h1>")

      const v1Js = await handler(
        new Request(`http://opencode.local/api/experimental/artifacts/${created.data.id}/view/assets/app.js`),
      )
      expect(v1Js.status).toBe(200)
      expect(await v1Js.text()).toBe("window.VERSION = 1")

      // Update to v2 keeps same ID/URL and preserves pinned ?version=1
      const updated = await api.experimental.artifacts.update({
        artifactID: created.data.id,
        baseVersion: 1,
        files: {
          "index.html": "<!doctype html><h1>v2</h1>",
        },
      })
      expect(updated.data.version).toBe(2)

      await expect(
        api.experimental.artifacts.update({
          artifactID: created.data.id,
          baseVersion: 1,
          files: { "index.html": "stale" },
        }),
      ).rejects.toMatchObject({ _tag: "ConflictError" })

      const latestHtml = await handler(
        new Request(`http://opencode.local/api/experimental/artifacts/${created.data.id}/view/`),
      )
      expect(await latestHtml.text()).toContain("<h1>v2</h1>")

      const pinnedV1 = await handler(
        new Request(`http://opencode.local/api/experimental/artifacts/${created.data.id}/view/?version=1`),
      )
      expect(await pinnedV1.text()).toContain("<h1>v1</h1>")

      // Markdown entry renders HTML shell by default and raw Markdown with ?raw=1
      const md = await api.experimental.artifacts.create({
        location: { directory: enabled },
        sessionID: session.id,
        title: "Design Note",
        files: { "index.md": "# Hello Markdown" },
      })
      const mdHtml = await handler(new Request(`http://opencode.local${md.data.url}`))
      expect(mdHtml.headers.get("content-type")).toContain("text/html")
      expect(await mdHtml.text()).toContain("# Hello Markdown")

      const mdRaw = await handler(new Request(`http://opencode.local${md.data.url}?raw=1`))
      expect(await mdRaw.text()).toBe("# Hello Markdown")

      // Per-request override to plugin-registered "r2" provider
      const remote = await api.experimental.artifacts.create({
        location: { directory: enabled },
        sessionID: session.id,
        provider: "r2",
        title: "Shared bundle",
        files: { "index.html": "<p>remote</p>" },
      })
      expect(remote.data).toMatchObject({
        provider: "r2",
        url: `https://artifacts.example.test/${remote.data.id}/`,
      })

      const fetchedRemote = await api.experimental.artifacts.get({
        artifactID: remote.data.id,
        location: { directory: enabled },
      })
      expect(fetchedRemote.data.files).toEqual({ "index.html": "<p>remote</p>" })

      const listedR2 = await api.experimental.artifacts.list({
        sessionID: session.id,
        provider: "r2",
      })
      expect(listedR2.data.map((item) => item.id)).toEqual([remote.data.id])

      // Delete removes record and returns 404 thereafter
      await api.experimental.artifacts.delete({ artifactID: created.data.id, location: { directory: enabled } })
      await expect(
        api.experimental.artifacts.get({ artifactID: created.data.id, location: { directory: enabled } }),
      ).rejects.toMatchObject({
        _tag: "ArtifactNotFoundError",
      })
    })
  }),
)
