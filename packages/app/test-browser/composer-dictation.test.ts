import { afterEach, describe, expect, jest, test } from "bun:test"
import { createRequire } from "node:module"
import { createStore } from "solid-js/store"
import { createComponent, render } from "solid-js/web"
import type { ComposerPersistedState } from "@/composer/types"
import type { HexDictationFinishResult, HexDictationStartResult } from "@/runtime/platform/platform"

// Bun does not compile Solid JSX. Compile the real ComposerEditor with the same presets as Vite.
const require = createRequire(import.meta.url)

const solid = createRequire(require.resolve("vite-plugin-solid"))

const { transformSync } = solid("@babel/core")

Bun.plugin.clearAll()

Bun.plugin({
  name: "composer-dictation-solid",
  setup(build) {
    build.onLoad({ filter: /\.tsx$/ }, async (args) => ({
      contents: transformSync(await Bun.file(args.path).text(), {
        filename: args.path,
        presets: [solid.resolve("babel-preset-solid"), solid.resolve("@babel/preset-typescript")],
      }).code,
      loader: "js",
    }))
  },
})

const { createHexDictation } = await import("@/composer/dictation")

const { createComposerEditor } = await import("@/composer/editor/interaction")

const { ComposerEditor } = await import("@/composer/editor/editor")

afterEach(() => {
  jest.useRealTimers()
})

const failureCases: Array<{ startResult: HexDictationStartResult; expected: string }> = [
  {
    startResult: { status: "error", code: "not-installed" },
    expected: "not-installed",
  },
  {
    startResult: { status: "error", code: "not-ready" },
    expected: "not-ready",
  },
  {
    startResult: { status: "error", code: "busy" },
    expected: "busy",
  },
]

describe("Composer Hex dictation button", () => {
  test("renders live waveform bar and timer while recording, inserts transcript on finish, and resets", async () => {
    jest.useFakeTimers()

    let resolveStart!: (value: HexDictationStartResult) => void
    let resolveFinish!: (value: HexDictationFinishResult) => void
    let pushLevel: ((level: number) => void) | undefined
    const notifications: string[] = []
    const host = document.createElement("div")
    document.body.appendChild(host)

    const [state, setState] = createStore<ComposerPersistedState>({
      prompt: [{ type: "text", content: "Hello", start: 0, end: 5 }],
      cursor: 5,
      context: { items: [] },
    })

    const dispose = render(() => {
      const dictation = createHexDictation({
        bridge: {
          start: () =>
            new Promise((resolve) => {
              resolveStart = resolve
            }),
          finish: () =>
            new Promise((resolve) => {
              resolveFinish = resolve
            }),
          cancel: async () => undefined,
          onLevel: (cb) => {
            pushLevel = cb

            return () => {
              pushLevel = undefined
            }
          },
        },
        insert: (text) => controller.insertDictation(text),
        focus: () => controller.restoreFocus(),
        notify: {
          notInstalled: () => notifications.push("not-installed"),
          notReady: () => notifications.push("not-ready"),
          busy: () => notifications.push("busy"),
          empty: () => notifications.push("empty"),
          failed: (msg) => notifications.push(`failed:${msg ?? ""}`),
        },
      })

      const controller = createComposerEditor({
        store: [state, setState],
        commands: () => [],
        context: () => [],
        searchContextFiles: () => [],
        view: {
          dictation,
          submit: {
            stopping: () => false,
            onSubmit: () => undefined,
            onStop: () => undefined,
          },
        },
      })

      return createComponent(ComposerEditor, { controller })
    }, host)

    try {
      const button = host.querySelector<HTMLButtonElement>('[data-action="composer-dictate"]')
      const editor = host.querySelector<HTMLDivElement>('[data-component="composer-editor"]')
      expect(button).not.toBeNull()
      expect(editor).not.toBeNull()
      expect(button!.getAttribute("aria-label")).toBe("Dictate with Hex")
      expect(button!.getAttribute("aria-pressed")).toBe("false")
      expect(host.querySelector('[data-slot="composer-dictation-bar"]')).toBeNull()

      button!.click()
      expect(button!.getAttribute("aria-label")).toBe("Starting Hex…")
      expect(button!.getAttribute("aria-busy")).toBe("true")

      resolveStart({ status: "recording" })
      await Promise.resolve()
      expect(button!.getAttribute("aria-label")).toBe("Finish dictation")
      expect(button!.getAttribute("aria-pressed")).toBe("true")
      expect(button!.getAttribute("aria-busy")).toBe("false")

      const bar = host.querySelector('[data-slot="composer-dictation-bar"]')
      const timer = host.querySelector('[data-slot="composer-dictation-timer"]')
      const canvas = host.querySelector('[data-slot="composer-dictation-waveform"] canvas')
      expect(bar).not.toBeNull()
      expect(canvas).not.toBeNull()
      expect(timer?.textContent).toBe("0:00")

      pushLevel?.(0.75)
      jest.advanceTimersByTime(2_000)
      expect(timer?.textContent).toBe("0:02")

      button!.click()
      expect(button!.getAttribute("aria-label")).toBe("Transcribing…")
      expect(button!.getAttribute("aria-busy")).toBe("true")
      expect(pushLevel).toBeUndefined()

      resolveFinish({ status: "transcribed", transcript: "world from Hex" })
      await Promise.resolve()

      expect(host.querySelector('[data-slot="composer-dictation-bar"]')).toBeNull()
      expect(editor!.textContent).toBe("Hello world from Hex")
      expect(state.cursor).toBe(20)
      expect(button!.getAttribute("aria-label")).toBe("Dictated with Hex")

      jest.advanceTimersToNextTimer()
      expect(button!.getAttribute("aria-label")).toBe("Dictate with Hex")
      expect(notifications).toEqual([])
    } finally {
      dispose()
      host.remove()
    }
  })

  test("cancel button and Escape both discard an active Hex recording before composer stop actions", async () => {
    let canceled = 0
    let stopped = 0
    const host = document.createElement("div")
    document.body.appendChild(host)

    const [state, setState] = createStore<ComposerPersistedState>({
      prompt: [{ type: "text", content: "", start: 0, end: 0 }],
      cursor: 0,
      context: { items: [] },
    })

    const dispose = render(() => {
      const dictation = createHexDictation({
        bridge: {
          start: async () => ({ status: "recording" }),
          finish: async () => ({ status: "empty" }),
          cancel: async () => {
            canceled++
          },
        },
        insert: () => undefined,
        focus: () => undefined,
        notify: {
          notInstalled: () => undefined,
          notReady: () => undefined,
          busy: () => undefined,
          empty: () => undefined,
          failed: () => undefined,
        },
      })

      const controller = createComposerEditor({
        store: [state, setState],
        commands: () => [],
        context: () => [],
        searchContextFiles: () => [],
        view: {
          dictation,
          submit: {
            stopping: () => false,
            working: () => true,
            onSubmit: () => undefined,
            onStop: () => {
              stopped++
            },
          },
        },
      })

      return createComponent(ComposerEditor, { controller })
    }, host)

    try {
      const button = host.querySelector<HTMLButtonElement>('[data-action="composer-dictate"]')!
      const editor = host.querySelector<HTMLDivElement>('[data-component="composer-editor"]')!

      button.click()
      await Promise.resolve()
      expect(button.getAttribute("aria-pressed")).toBe("true")

      const cancelButton = host.querySelector<HTMLButtonElement>('[data-action="composer-dictate-cancel"]')
      expect(cancelButton).not.toBeNull()
      cancelButton!.click()
      expect(button.getAttribute("aria-pressed")).toBe("false")
      expect(host.querySelector('[data-slot="composer-dictation-bar"]')).toBeNull()
      expect(canceled).toBe(1)

      button.click()
      await Promise.resolve()
      expect(button.getAttribute("aria-pressed")).toBe("true")

      editor.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }))
      expect(button.getAttribute("aria-pressed")).toBe("false")
      expect(canceled).toBe(2)
      expect(stopped).toBe(0)
    } finally {
      dispose()
      host.remove()
    }
  })

  test.each(failureCases)("surfaces $expected when start fails", async ({ startResult, expected }) => {
    const notifications: string[] = []
    const host = document.createElement("div")
    document.body.appendChild(host)

    const [state, setState] = createStore<ComposerPersistedState>({
      prompt: [{ type: "text", content: "", start: 0, end: 0 }],
      cursor: 0,
      context: { items: [] },
    })

    const dispose = render(() => {
      const dictation = createHexDictation({
        bridge: {
          start: async () => startResult,
          finish: async () => ({ status: "empty" }),
          cancel: async () => undefined,
        },
        insert: () => undefined,
        focus: () => undefined,
        notify: {
          notInstalled: () => notifications.push("not-installed"),
          notReady: () => notifications.push("not-ready"),
          busy: () => notifications.push("busy"),
          empty: () => notifications.push("empty"),
          failed: (msg) => notifications.push(`failed:${msg ?? ""}`),
        },
      })

      const controller = createComposerEditor({
        store: [state, setState],
        commands: () => [],
        context: () => [],
        searchContextFiles: () => [],
        view: {
          dictation,
          submit: {
            stopping: () => false,
            onSubmit: () => undefined,
            onStop: () => undefined,
          },
        },
      })

      return createComponent(ComposerEditor, { controller })
    }, host)

    try {
      const button = host.querySelector<HTMLButtonElement>('[data-action="composer-dictate"]')!
      button.click()
      await Promise.resolve()

      expect(button.getAttribute("aria-label")).toBe("Dictate with Hex")
      expect(notifications).toEqual([expected])
    } finally {
      dispose()
      host.remove()
    }
  })
})
