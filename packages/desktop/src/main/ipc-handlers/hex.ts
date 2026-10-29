import { Effect } from "effect"
import { HexRpcs } from "../../shared/ipc-rpc"
import { HexDictationLevel } from "../../shared/ipc-rpc/events"
import { emitIpcEvent } from "../ipc-events"
import { IpcPortHandoff } from "../ipc-transport"
import { HexDictation } from "../native/hex"
import { sender } from "./context"

export const hexHandlers = HexRpcs.toLayer(
  Effect.gen(function* () {
    const hex = yield* HexDictation.Service
    const handoff = yield* IpcPortHandoff
    const runFork = Effect.runForkWith(yield* Effect.context())
    // Windows whose recording ends when they close.
    const watched = new Set<number>()

    return HexRpcs.of({
      HexDictationStart: (_args, context) => {
        const contents = sender(handoff, context)
        const id = contents.id

        if (!watched.has(id)) {
          watched.add(id)
          contents.once("destroyed", () => {
            watched.delete(id)
            runFork(hex.cancel(id))
          })
        }

        return hex.start(id, (level) => {
          if (!contents.isDestroyed()) emitIpcEvent(contents, new HexDictationLevel({ level }))
        })
      },
      HexDictationFinish: (_args, context) => hex.finish(sender(handoff, context).id),
      HexDictationCancel: (_args, context) => {
        const contents = handoff.sender(context.client.id)

        return contents && !contents.isDestroyed() ? hex.cancel(contents.id) : Effect.void
      },
    })
  }),
)
