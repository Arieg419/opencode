import { batch, onCleanup } from "solid-js"
import { createStore } from "solid-js/store"
import type { HexDictationBridge } from "@/runtime/platform/platform"
import type { ComposerDictationControl, ComposerDictationStatus } from "./editor/interaction"

const DONE_RESET_MS = 1_500

const ELAPSED_TICK_MS = 1_000

export function createHexDictation(input: {
  bridge: HexDictationBridge
  insert: (transcript: string) => void
  focus: () => void
  notify: {
    notInstalled: () => void
    notReady: () => void
    busy: () => void
    empty: () => void
    failed: (message?: string) => void
  }
}): ComposerDictationControl {
  const [state, setState] = createStore<{
    status: ComposerDictationStatus
    level: number
    elapsedSeconds: number
  }>({
    status: "idle",
    level: 0,
    elapsedSeconds: 0,
  })

  let generation = 0
  let doneTimer: ReturnType<typeof setTimeout> | undefined
  let elapsedTimer: ReturnType<typeof setInterval> | undefined
  let stopLevels: (() => void) | undefined

  const clearDoneTimer = () => {
    if (!doneTimer) return
    clearTimeout(doneTimer)
    doneTimer = undefined
  }

  const stopRecordingTelemetry = () => {
    if (elapsedTimer) {
      clearInterval(elapsedTimer)
      elapsedTimer = undefined
    }

    if (stopLevels) {
      stopLevels()
      stopLevels = undefined
    }

    setState("level", 0)
  }

  const startRecordingTelemetry = (current: number) => {
    stopRecordingTelemetry()
    setState("elapsedSeconds", 0)
    stopLevels = input.bridge.onLevel?.((level) => {
      if (current === generation && state.status === "recording") {
        setState("level", Math.max(0, Math.min(1, level)))
      }
    })
    elapsedTimer = setInterval(() => {
      if (current !== generation || state.status !== "recording") return
      setState("elapsedSeconds", (seconds) => seconds + 1)
    }, ELAPSED_TICK_MS)
  }

  const resetIdle = () => {
    stopRecordingTelemetry()
    batch(() => {
      setState("status", "idle")
      setState("elapsedSeconds", 0)
    })
  }

  const cancel = () => {
    clearDoneTimer()
    const active = state.status === "recording" || state.status === "starting"
    generation++
    resetIdle()

    if (active) void input.bridge.cancel().catch(() => undefined)
  }

  onCleanup(cancel)

  const toggle = () => {
    if (state.status === "starting" || state.status === "transcribing") return
    clearDoneTimer()

    if (state.status === "recording") {
      const current = ++generation
      stopRecordingTelemetry()
      setState("status", "transcribing")
      void input.bridge
        .finish()
        .then((result) => {
          if (current !== generation) return

          if (result.status === "transcribed") {
            input.insert(result.transcript)
            batch(() => {
              setState("status", "done")
              setState("elapsedSeconds", 0)
            })
            doneTimer = setTimeout(() => {
              doneTimer = undefined

              if (current === generation && state.status === "done") setState("status", "idle")
            }, DONE_RESET_MS)

            return
          }

          resetIdle()

          if (result.status === "empty") {
            input.notify.empty()

            return
          }

          input.notify.failed(result.message)
        })
        .catch((error) => {
          if (current !== generation) return
          resetIdle()
          input.notify.failed(error instanceof Error ? error.message : String(error))
        })

      return
    }

    const current = ++generation
    batch(() => {
      setState("status", "starting")
      setState("level", 0)
      setState("elapsedSeconds", 0)
    })
    void input.bridge
      .start()
      .then((result) => {
        if (current !== generation) return

        if (result.status === "recording") {
          setState("status", "recording")
          startRecordingTelemetry(current)
          input.focus()

          return
        }

        resetIdle()

        if (result.code === "not-installed") {
          input.notify.notInstalled()

          return
        }

        if (result.code === "not-ready") {
          input.notify.notReady()

          return
        }

        if (result.code === "busy") {
          input.notify.busy()

          return
        }

        input.notify.failed(result.message)
      })
      .catch((error) => {
        if (current !== generation) return
        resetIdle()
        input.notify.failed(error instanceof Error ? error.message : String(error))
      })
  }

  return {
    status: () => state.status,
    level: () => state.level,
    elapsedSeconds: () => state.elapsedSeconds,
    onToggle: toggle,
    onCancel: cancel,
  }
}
