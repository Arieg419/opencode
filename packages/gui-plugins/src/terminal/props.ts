import type { ServerRef } from "@opencode/plugin/gui"
import type { TerminalModel } from "./model"
import type { TerminalQuoteTarget } from "./quote"

export type TerminalProps = {
  ptyID: string
  server: ServerRef
  ghostty: TerminalModel["ghostty"]
  quote?: TerminalQuoteTarget
  autoFocus?: boolean
  onAutoFocus?: () => void
  onSubmit?: () => void
  class?: string
}
