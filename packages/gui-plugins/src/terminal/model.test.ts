import { describe, expect, test } from "bun:test"
import { createStore, produce } from "solid-js/store"
import { ClientError, type PersistentPtyInfo } from "@opencode/client"
import type { Layout, ServerRef, SessionRef, Storage, ToastOptions } from "@opencode/plugin/gui"
import { createTerminalModel } from "./model"

const flush = () => new Promise((resolve) => setTimeout(resolve, 0))

const info = (id: string, title: string, sessionID = "ses_1"): PersistentPtyInfo => ({
  id,
  title,
  userTitle: null,
  command: "/bin/zsh",
  args: [],
  cwd: "/repo",
  status: "running",
  pid: 1,
  sessionID,
  foregroundProcess: null,
  size: { cols: 80, rows: 24 },
  output: { head: 0, tail: 0 },
})

// A server with persistent terminals: its own terminal list per session, and an event stream the test drives.
function fakeServer(options: { capabilities?: { persistentPty?: boolean } } = {}) {
  const terminals = new Map<string, PersistentPtyInfo[]>()
  const handlers = new Map<string, ((event: { data: { sessionID: string } }) => void)[]>()
  const listeners: ((event: { name: string }) => void)[] = []
  const calls = { list: 0, create: [] as unknown[], remove: [] as string[] }
  const state = {
    capabilities: options.capabilities,
    directory: "/repo",
    created: 0,
    createError: undefined as unknown,
    list: undefined as Promise<void> | undefined,
  }

  const server = {
    id: "http://server",
    data: {
      on: (type: string, handler: (event: { data: { sessionID: string } }) => void) => {
        handlers.set(type, [...(handlers.get(type) ?? []), handler])

        return () =>
          handlers.set(
            type,
            (handlers.get(type) ?? []).filter((item) => item !== handler),
          )
      },
      listen: (handler: (event: { name: string }) => void) => {
        listeners.push(handler)

        return () => listeners.splice(listeners.indexOf(handler), 1)
      },
      session: { get: (id: string) => ({ id, location: { directory: state.directory } }) },
    },
    client: {
      server: { info: async () => ({ capabilities: state.capabilities }) },
      experimental: {
        persistentPty: {
          list: async (input: { sessionID: string }) => {
            calls.list += 1
            await state.list

            return [...(terminals.get(input.sessionID) ?? [])]
          },
          create: async (input: { sessionID: string; title: string; cwd?: string }) => {
            calls.create.push(input)

            if (state.createError) throw state.createError

            state.created += 1
            const created = info(`pty_new${state.created}`, input.title, input.sessionID)
            terminals.set(input.sessionID, [...(terminals.get(input.sessionID) ?? []), created])

            return created
          },
          remove: async (input: { ptyID: string }) => {
            calls.remove.push(input.ptyID)
            terminals.forEach((list, sessionID) =>
              terminals.set(
                sessionID,
                list.filter((item) => item.id !== input.ptyID),
              ),
            )
          },
        },
      },
    },
  }

  return {
    // SAFETY: the model reads only `id`, `data` (on, listen, session.get) and the client calls stubbed here.
    ref: server as unknown as ServerRef,
    terminals,
    calls,
    state,
    emit(type: string, sessionID = "ses_1") {
      handlers.get(type)?.forEach((handler) => handler({ data: { sessionID } }))
      listeners.forEach((listener) => listener({ name: type }))
    },
  }
}

function setup(server: ReturnType<typeof fakeServer>) {
  const stored = new Map<string, string[]>()
  const opened: { key: string; session: string; tab?: string }[] = []
  const toasts: ToastOptions[] = []
  const removed: { key: string; options: unknown }[] = []

  const storage = {
    memory<T extends object>(_key: string, options: { initial: T }) {
      const [value, set] = createStore(options.initial)

      return [value, (mutate: (draft: T) => void) => set(produce(mutate))] as const
    },
    store<T extends object>(_key: string, options: { initial: T }) {
      const [value, set] = createStore({ value: options.initial })

      return {
        get value() {
          return value.value
        },
        ready: () => true,
        set: (next: T) => set("value", next),
        update: () => undefined,
      }
    },
    remove: (key: string, options: unknown) => void removed.push({ key, options }),
  }

  const layout: Pick<Layout, "open" | "stored"> = {
    stored: (session) => stored.get(session.key) ?? [],
    open(key, session, options) {
      opened.push({ key, session: session.key, tab: options?.tab })
      stored.set(session.key, [...(stored.get(session.key) ?? []), key.slice("terminal:".length)])
    },
  }

  const model = createTerminalModel({
    // SAFETY: the model opens only `memory`, `store` and `remove`, which these stand-ins implement.
    storage: storage as unknown as Storage,
    layout,
    toast: { show: (options) => (toasts.push(options), { dismiss: () => undefined }) },
    t: (key) => key,
    tab: (id) => `terminal:${id}`,
  })

  const session: SessionRef = {
    key: `${server.ref.id}\nses_1`,
    id: "ses_1",
    tab: "tab_1",
    server: server.ref,
    pending: false,
    location: { directory: "/repo" },
  }

  return { model, session, stored, opened, toasts, removed }
}

describe("terminal model", () => {
  test("is ready only once the server answers the session's first list", async () => {
    const server = fakeServer()
    const list = Promise.withResolvers<void>()
    server.state.list = list.promise
    server.terminals.set("ses_1", [info("pty_1", "Terminal 1")])
    const { model, session } = setup(server)

    const terminal = model.load(session)
    await flush()

    // An empty list before the first answer would let the transient strip drop restored terminal tabs.
    expect(terminal.ready()).toBe(false)
    expect(terminal.all()).toEqual([])

    list.resolve()
    await flush()

    expect(terminal.ready()).toBe(true)
    expect(terminal.all().map((item) => item.id)).toEqual(["pty_1"])
  })

  test("mirrors the server's list on terminal events and reconnects", async () => {
    const server = fakeServer()
    server.terminals.set("ses_1", [info("pty_1", "Terminal 1"), info("pty_2", "Terminal 2")])
    const { model, session } = setup(server)
    const terminal = model.load(session)
    await flush()

    server.terminals.set("ses_1", [info("pty_2", "Terminal 2")])
    server.emit("persistent-pty.removed")
    await flush()

    // The panel lists only terminals the model knows, so the removed terminal's tab leaves the strip.
    expect(terminal.get("pty_1")).toBeUndefined()
    expect(terminal.all().map((item) => item.id)).toEqual(["pty_2"])

    server.terminals.set("ses_1", [info("pty_2", "build")])
    server.emit("persistent-pty.updated")
    await flush()

    expect(terminal.get("pty_2")?.title).toBe("build")

    // Terminals that changed while the event stream was down show up after it reconnects.
    server.terminals.set("ses_1", [info("pty_2", "build"), info("pty_3", "Terminal 3")])
    server.emit("server.connected")
    await flush()

    expect(terminal.all().map((item) => item.id)).toEqual(["pty_2", "pty_3"])

    // Another session's events leave this one alone.
    const calls = server.calls.list
    server.emit("persistent-pty.added", "ses_other")
    await flush()

    expect(server.calls.list).toBe(calls)
  })

  test("adds terminals another client created to the end of the strip without selecting them", async () => {
    const server = fakeServer()
    server.terminals.set("ses_1", [info("pty_1", "Terminal 1")])
    const { model, session, stored, opened } = setup(server)
    stored.set(session.key, ["pty_1"])
    const terminal = model.load(session)
    await flush()

    expect(opened).toEqual([])

    server.terminals.set("ses_1", [info("pty_1", "Terminal 1"), info("pty_tui", "Terminal")])
    server.emit("persistent-pty.added")
    await flush()

    expect(terminal.all().map((item) => item.id)).toEqual(["pty_1", "pty_tui"])
    expect(opened).toEqual([{ key: "terminal:pty_tui", session: session.key, tab: "append" }])
  })

  test("hides terminals on servers without persistent terminal support until a reconnect reports it", async () => {
    const server = fakeServer({ capabilities: { persistentPty: false } })
    server.terminals.set("ses_1", [info("pty_1", "Terminal 1")])
    const { model, session } = setup(server)

    const terminal = model.load(session)
    await flush()

    expect(model.available(server.ref)).toBe(false)
    expect(server.calls.list).toBe(0)
    expect(terminal.ready()).toBe(false)

    // Servers that predate the capability omit it and still offer terminals.
    server.state.capabilities = undefined
    server.emit("server.connected")
    await flush()

    expect(model.available(server.ref)).toBe(true)
    expect(terminal.all().map((item) => item.id)).toEqual(["pty_1"])
  })

  test("creates the next unused default title in the session's current directory and selects it", async () => {
    const server = fakeServer()
    server.terminals.set("ses_1", [info("pty_1", "Terminal 1"), info("pty_3", "Terminal 3"), info("pty_x", "vim")])
    const { model, session } = setup(server)
    const terminal = model.load(session)
    await flush()
    server.state.directory = "/repo/worktree"

    const id = await terminal.new()

    expect(server.calls.create).toEqual([
      { sessionID: "ses_1", args: [], cwd: "/repo/worktree", title: "Terminal 2", env: {} },
    ])
    expect(id).toBe("pty_new1")
    expect(terminal.active()).toBe("pty_new1")
    expect(terminal.get("pty_new1")?.title).toBe("Terminal 2")
  })

  test("tells the user to update a server too old for persistent terminals", async () => {
    const server = fakeServer()
    server.state.createError = new ClientError("UnexpectedStatus", { cause: { status: 404 }, detail: "404" })
    const { model, session, toasts } = setup(server)
    const terminal = model.load(session)
    await flush()

    expect(await terminal.new()).toBeUndefined()
    expect(toasts).toEqual([{ variant: "error", title: "create.unsupported" }])
  })

  test("closes a terminal on the server and drops it once the server no longer lists it", async () => {
    const server = fakeServer()
    server.terminals.set("ses_1", [info("pty_1", "Terminal 1"), info("pty_2", "Terminal 2")])
    const { model, session } = setup(server)
    const terminal = model.load(session)
    await flush()
    terminal.select("pty_1")

    await terminal.close("pty_1")

    expect(server.calls.remove).toEqual(["pty_1"])
    expect(terminal.all().map((item) => item.id)).toEqual(["pty_2"])
    expect(terminal.active()).toBe("pty_2")
  })

  test("drops the terminals stored before they lived on the server", async () => {
    const server = fakeServer()
    const { model, session, removed } = setup(server)
    model.load(session)
    await flush()

    expect(removed).toEqual([
      {
        key: "terminals",
        options: { scope: { server: server.ref.id, directory: "/repo" }, from: "workspace:terminal" },
      },
    ])
  })
})
