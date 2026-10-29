import { afterEach, describe, expect, jest, test } from "bun:test"
import { createRequire } from "node:module"
import { createStore } from "solid-js/store"
import { createComponent, render } from "solid-js/web"
import type { ComposerPersistedState } from "@/composer/types"
import type {
  HexDictationBridge,
  HexDictationFinishResult,
  HexDictationStartResult,
} from "@/runtime/platform/platform"

// Bun does not compile Solid JSX. Compile the real ComposerEditor with the same presets as Vite.
const require = createRequire(import.meta.url)

const solid = createRequire(require.resolve("vite-plugin-solid"))

const { transformSync } = solid("@babel/core")

// This file runs with --isolate. Replace the shared hyperscript preload so Solid owns refs and mount effects; the
// filter covers every TSX module the editor imports.
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
  { startResult: { status: "error", code: "not-installed" }, expected: "not-installed" },
  { startResult: { status: "error", code: "not-ready" }, expected: "not-ready" },
  { startResult: { status: "error", code: "busy" }, expected: "busy" },
]

describe("Composer Hex dictation button", () => {
  test("renders live waveform bar and timer while recording, inserts transcript on finish, and resets", async () => {
    jest.useFakeTimers()

    let resolveStart!: (value: HexDictationStartResult) => void
    let resolveFinish!: (value: HexDictationFinishResult) => void
    let pushLevel: ((level: number) => void) | undefined

    const app = mount(
      {
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
      { text: "Hello" },
    )

    try {
      const button = app.button()
      expect(button.getAttribute("aria-label")).toBe("Dictate with Hex")
      expect(button.getAttribute("aria-pressed")).toBe("false")
      expect(app.bar()).toBeNull()

      button.click()
      expect(button.getAttribute("aria-label")).toBe("Starting Hex…")
      expect(button.getAttribute("aria-busy")).toBe("true")

      resolveStart({ status: "recording" })
      await Promise.resolve()
      expect(button.getAttribute("aria-label")).toBe("Finish dictation")
      expect(button.getAttribute("aria-pressed")).toBe("true")
      expect(button.getAttribute("aria-busy")).toBe("false")

      const timer = app.host.querySelector('[data-slot="composer-dictation-timer"]')
      expect(app.bar()).not.toBeNull()
      expect(app.host.querySelector('[data-slot="composer-dictation-waveform"] canvas')).not.toBeNull()
      expect(timer?.textContent).toBe("0:00")

      pushLevel?.(0.75)
      jest.advanceTimersByTime(2_000)
      expect(timer?.textContent).toBe("0:02")

      button.click()
      expect(button.getAttribute("aria-label")).toBe("Transcribing…")
      expect(button.getAttribute("aria-busy")).toBe("true")
      expect(pushLevel).toBeUndefined()

      resolveFinish({ status: "transcribed", transcript: "world from Hex" })
      await Promise.resolve()

      expect(app.bar()).toBeNull()
      expect(app.editor().textContent).toBe("Hello world from Hex")
      expect(app.state.cursor).toBe(20)
      expect(button.getAttribute("aria-label")).toBe("Dictated with Hex")

      jest.advanceTimersToNextTimer()
      expect(button.getAttribute("aria-label")).toBe("Dictate with Hex")
      expect(app.notifications).toEqual([])
    } finally {
      app.dispose()
    }
  })

  test("cancel button and Escape both discard an active Hex recording before composer stop actions", async () => {
    let canceled = 0
    let stopped = 0

    const app = mount(
      {
        start: async () => ({ status: "recording" }),
        finish: async () => ({ status: "empty" }),
        cancel: async () => {
          canceled++
        },
        onLevel: () => () => undefined,
      },
      {
        onStop: () => {
          stopped++
        },
      },
    )

    try {
      const button = app.button()

      button.click()
      await Promise.resolve()
      expect(button.getAttribute("aria-pressed")).toBe("true")

      app.host.querySelector<HTMLButtonElement>('[data-action="composer-dictate-cancel"]')!.click()
      expect(button.getAttribute("aria-pressed")).toBe("false")
      expect(app.bar()).toBeNull()
      expect(canceled).toBe(1)

      button.click()
      await Promise.resolve()
      expect(button.getAttribute("aria-pressed")).toBe("true")

      app.editor().dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }))
      expect(button.getAttribute("aria-pressed")).toBe("false")
      expect(canceled).toBe(2)
      expect(stopped).toBe(0)
    } finally {
      app.dispose()
    }
  })

  test.each(failureCases)("surfaces $expected when start fails", async ({ startResult, expected }) => {
    const app = mount({
      start: async () => startResult,
      finish: async () => ({ status: "empty" }),
      cancel: async () => undefined,
      onLevel: () => () => undefined,
    })

    try {
      app.button().click()
      await Promise.resolve()

      expect(app.button().getAttribute("aria-label")).toBe("Dictate with Hex")
      expect(app.notifications).toEqual([expected])
    } finally {
      app.dispose()
    }
  })
})

/** The real editor and dictation over a stub Hex bridge; with onStop, the session is working. */
function mount(bridge: HexDictationBridge, options: { text?: string; onStop?: () => void } = {}) {
  const text = options.text ?? ""
  const notifications: string[] = []
  const host = document.createElement("div")
  document.body.appendChild(host)

  const [state, setState] = createStore<ComposerPersistedState>({
    prompt: [{ type: "text", content: text, start: 0, end: text.length }],
    cursor: text.length,
    context: { items: [] },
  })

  const dispose = render(() => {
    const dictation = createHexDictation({
      bridge,
      insert: (transcript) => controller.insertDictation(transcript),
      focus: () => controller.restoreFocus(),
      notify: (problem) => notifications.push(problem),
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
          working: () => !!options.onStop,
          onSubmit: () => undefined,
          onStop: options.onStop ?? (() => undefined),
        },
      },
    })

    return createComponent(ComposerEditor, { controller })
  }, host)

  return {
    host,
    state,
    notifications,
    button: () => host.querySelector<HTMLButtonElement>('[data-action="composer-dictate"]')!,
    editor: () => host.querySelector<HTMLDivElement>('[data-component="composer-editor"]')!,
    bar: () => host.querySelector('[data-slot="composer-dictation-bar"]'),
    dispose() {
      dispose()
      host.remove()
    },
  }
}
