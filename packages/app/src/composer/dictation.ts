import { batch, onCleanup } from "solid-js"
import { createStore } from "solid-js/store"
import type { HexDictationBridge } from "@/runtime/platform/platform"
import type { ComposerDictationControl, ComposerDictationStatus } from "./editor/interaction"

const DONE_RESET_MS = 1_500

export type HexDictationProblem = "not-installed" | "not-ready" | "busy" | "empty" | "failed"

export function createHexDictation(input: {
  bridge: HexDictationBridge
  insert: (transcript: string) => void
  focus: () => void
  notify: (problem: HexDictationProblem, message?: string) => void
}): ComposerDictationControl {
  const [state, setState] = createStore<{ status: ComposerDictationStatus; level: number; elapsedSeconds: number }>({
    status: "idle",
    level: 0,
    elapsedSeconds: 0,
  })

  // Each start, finish, and cancel takes a new generation, so a reply to an older one is ignored.
  let generation = 0
  let doneTimer: ReturnType<typeof setTimeout> | undefined
  let elapsedTimer: ReturnType<typeof setInterval> | undefined
  let stopLevels: (() => void) | undefined

  const stopRecording = () => {
    clearInterval(elapsedTimer)
    stopLevels?.()
    stopLevels = undefined
  }

  const settle = (status: ComposerDictationStatus) => {
    stopRecording()
    clearTimeout(doneTimer)
    setState("status", status)
  }

  // The bridge rejects only when the desktop could not answer.
  const fail = (current: number, error: Error) => {
    if (current !== generation) return
    settle("idle")
    input.notify("failed", error.message)
  }

  const cancel = () => {
    const active = state.status === "recording" || state.status === "starting"
    generation++
    settle("idle")

    if (active) void input.bridge.cancel().catch(() => undefined)
  }

  onCleanup(cancel)

  const finish = () => {
    const current = ++generation
    settle("transcribing")
    void input.bridge
      .finish()
      .then((result) => {
        if (current !== generation) return

        if (result.status !== "transcribed") {
          settle("idle")

          return result.status === "empty" ? input.notify("empty") : input.notify("failed", result.message)
        }

        input.insert(result.transcript)
        settle("done")
        doneTimer = setTimeout(() => setState("status", "idle"), DONE_RESET_MS)
      })
      .catch((error: Error) => fail(current, error))
  }

  const start = () => {
    const current = ++generation
    batch(() => {
      settle("starting")
      setState("level", 0)
      setState("elapsedSeconds", 0)
    })
    void input.bridge
      .start()
      .then((result) => {
        if (current !== generation) return

        if (result.status !== "recording") {
          settle("idle")

          return input.notify(result.code, result.message)
        }

        setState("status", "recording")
        stopLevels = input.bridge.onLevel((level) => setState("level", Math.max(0, Math.min(1, level))))
        elapsedTimer = setInterval(() => setState("elapsedSeconds", (seconds) => seconds + 1), 1_000)
        input.focus()
      })
      .catch((error: Error) => fail(current, error))
  }

  return {
    status: () => state.status,
    level: () => state.level,
    elapsedSeconds: () => state.elapsedSeconds,
    onToggle: () => {
      if (state.status === "recording") return finish()

      if (state.status === "idle" || state.status === "done") start()
    },
    onCancel: cancel,
  }
}
