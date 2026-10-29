import { describe, expect, test } from "bun:test"
import { Option } from "effect"
import { decodePersistentPtyMessage, persistentPtyFrame } from "../src/solid"

const info = {
  id: "pty_1",
  title: "zsh",
  command: "/bin/zsh",
  args: [],
  cwd: "/repo",
  status: "running",
  pid: 42,
  sessionID: "ses_1",
  foregroundProcess: null,
  size: { cols: 80, rows: 24 },
  output: { head: 0, tail: 128 },
}

const decode = (value: object) => Option.getOrUndefined(decodePersistentPtyMessage(JSON.stringify(value)))

describe("decodePersistentPtyMessage", () => {
  test("decodes every message the server sends", () => {
    const messages = [
      {
        type: "attached",
        inputProtocol: 1,
        info,
        role: "controller",
        generation: 3,
        replay: { requestedOffset: 128, availableOffset: 0, endOffset: 256, truncated: false },
      },
      { type: "replay_complete", endOffset: 256 },
      { type: "resized", cols: 100, rows: 30, generation: 4, checkpoint: "G2M=" },
      { type: "controller_changed", attachmentID: "attachment-1", generation: 5 },
      { type: "exited", exitCode: 0, finalOffset: 512 },
      { type: "title_changed", title: "vim" },
      { type: "foreground_process_changed", process: "vim" },
      { type: "foreground_process_changed", process: null },
    ]

    expect(messages.map(decode)).toEqual(messages)
  })

  test("decodes the fields JSON.stringify drops", () => {
    expect(decode({ type: "controller_changed", generation: 5 })).toEqual({ type: "controller_changed", generation: 5 })
    expect(decode({ type: "exited", finalOffset: 512 })).toEqual({ type: "exited", finalOffset: 512 })
  })

  test("ignores fields it does not know", () => {
    expect(decode({ type: "replay_complete", endOffset: 1, extra: true })).toEqual({
      type: "replay_complete",
      endOffset: 1,
    })
  })

  test("returns None for unknown message types", () => {
    expect(Option.isNone(decodePersistentPtyMessage(JSON.stringify({ type: "future_message", value: 1 })))).toBe(true)
  })

  test("returns None for malformed input", () => {
    const inputs = [
      "",
      "not json",
      "null",
      "42",
      JSON.stringify({ endOffset: 1 }),
      JSON.stringify({ type: "resized", cols: "100", rows: 30, generation: 4, checkpoint: "" }),
      JSON.stringify({ type: "resized", cols: 100, rows: 30, generation: 4 }),
      JSON.stringify({ type: "attached", inputProtocol: 1, role: "controller", generation: 1 }),
      JSON.stringify({ type: "foreground_process_changed" }),
    ]

    expect(inputs.map((input) => Option.isNone(decodePersistentPtyMessage(input)))).toEqual(inputs.map(() => true))
  })
})

describe("persistentPtyFrame", () => {
  test("encodes a size-only interaction frame", () => {
    expect(Array.from(persistentPtyFrame({ cols: 300, rows: 24 }))).toEqual([0, 1, 44, 0, 24])
  })

  test("encodes an input frame with its data", () => {
    expect(Array.from(persistentPtyFrame({ cols: 80, rows: 513 }, new Uint8Array([104, 105])))).toEqual([
      1, 0, 80, 2, 1, 104, 105,
    ])
  })

  test("encodes empty input as an input frame", () => {
    expect(Array.from(persistentPtyFrame({ cols: 80, rows: 24 }, new Uint8Array()))).toEqual([1, 0, 80, 0, 24])
  })
})
