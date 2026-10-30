import { createSimpleContext } from "@opencode/ui/context"
import { useDialog } from "@opencode/ui/context/dialog"
import { Schema } from "effect"
import { useLocation, useNavigate } from "@solidjs/router"
import { onCleanup } from "solid-js"
import { createStore } from "solid-js/store"
import type { SessionInfo } from "@opencode/client/promise"
import { usePlatform } from "@/runtime/platform/platform"
import { Persist, persisted } from "@/runtime/persistence/storage"
import { Persistence } from "@/runtime/persistence/schema"
import type { ServerCtx } from "@/runtime/server/runtime"
import type { ServerConnection } from "@/runtime/server/registry"
import { sessionHref } from "@/shell/routes/session"
import { findSessionTab, useTabs } from "@/shell/tabs/tabs"
import type { CallState, createVoiceCall } from "./call"

export const { provider: VoiceProvider, use: useVoice } = createSimpleContext({
  name: "Voice",
  init: () => {
    const platform = usePlatform()
    const navigate = useNavigate()
    const location = useLocation()
    const tabs = useTabs()

    const [state, setState] = createStore<
      CallState & {
        server?: ServerConnection.Key
        captions: { utterance: number; speaker: CallState["speaker"]; text: string }[]
        /** Whether the transcript above the composer is open. */
        transcript: boolean
      }
    >({
      status: "idle",
      muted: false,
      caption: "",
      speaker: "assistant",
      title: "",
      captions: [],
      transcript: false,
    })

    // Written by the call several times a second and read by the scope every frame, so it stays out of the store.
    const meter = { mic: 0, speaker: 0 }

    const current: { call?: ReturnType<typeof createVoiceCall>; generation: number } = { generation: 0 }
    // Where the latest call was placed and the session it follows, for setting it up again after it drops.
    const target: { server?: ServerConnection.Key; ctx?: ServerCtx; session?: SessionInfo } = {}
    const dialog = useDialog()

    const [onboarding, setOnboarding] = persisted(
      Persist.global("voice.onboarding"),
      Persistence.struct({ done: Schema.Boolean }),
      { done: false },
    )

    // Resolves once the router shows href; voice must not report a move that didn't happen.
    const routed = (href: string, signal: AbortSignal) =>
      new Promise<void>((resolve, reject) => {
        const started = Date.now()

        const check = () => {
          if (signal.aborted) return reject(signal.reason)

          if (location.pathname === href) return resolve()

          if (Date.now() - started > 3000) return reject(new Error("Desktop did not show the session"))
          setTimeout(check, 50)
        }

        check()
      })

    const end = async () => {
      const generation = ++current.generation
      const call = current.call
      current.call = undefined
      await call?.end()

      if (current.generation !== generation) return
      setState({ status: "idle", server: undefined, caption: "", captions: [], muted: false, since: undefined })
      meter.mic = 0
      meter.speaker = 0
    }

    const unload = () => {
      void end()
    }

    window.addEventListener("pagehide", unload)
    window.addEventListener("beforeunload", unload)
    onCleanup(() => {
      window.removeEventListener("pagehide", unload)
      window.removeEventListener("beforeunload", unload)
      void end()
    })

    // The call reports each utterance as it grows: its line is extended in place, and the next utterance starts a
    // new one.
    const caption = (change: Partial<CallState>) => {
      const text = change.caption
      const utterance = change.utterance

      if (!text || utterance === undefined) return
      const last = state.captions.length - 1

      if (state.captions[last]?.utterance === utterance) return setState("captions", last, "text", text)
      setState("captions", (list) => [...list, { utterance, speaker: state.speaker, text }].slice(-40))
    }

    const start = async (server: ServerConnection.Key, ctx: ServerCtx, session: SessionInfo) => {
      if (state.status !== "idle") return
      target.server = server
      target.ctx = ctx
      target.session = session
      const generation = ++current.generation
      setState({
        status: "connecting",
        server,
        title: session.title ?? session.id,
        failure: undefined,
        since: undefined,
        caption: "",
        captions: [],
      })
      await import("./call")
        .then(async ({ createVoiceCall }) => {
          if (current.generation !== generation) return
          current.call = createVoiceCall({
            ctx,
            session,
            platform,
            open: async (next, signal) => {
              const href = sessionHref(server, next.id)
              // Subagent sessions open inside their root session's tab, which the titlebar finds from the route.
              const tab =
                findSessionTab(tabs.store, server, next.id) ??
                (next.parentID ? undefined : tabs.addSessionTab({ server, sessionId: next.id }))

              if (tab) tabs.remember(tab)
              navigate(href)
              await routed(href, signal)
            },
            tabs: () =>
              tabs.store.flatMap((tab) => (tab.type === "session" && tab.server === server ? [tab.sessionId] : [])),
            change: (change) => {
              if (current.generation !== generation) return
              setState(change)
              caption(change)
            },
            levels: (mic, speaker) => {
              if (current.generation !== generation) return
              meter.mic = mic
              meter.speaker = speaker
            },
          })
          await current.call.join()
        })
        .catch(() => {
          if (current.generation === generation) setState({ status: "dropped", failure: "connection" })
        })
    }

    // The first call opens the onboarding screen; later ones start at once. A call the server can't place opens it
    // again from the dropped bar, in its setup state.
    const onboard = (server: ServerConnection.Key, ctx: ServerCtx, session: SessionInfo) =>
      void import("./onboarding").then(({ DialogVoiceOnboarding }) =>
        dialog.show(() => (
          <DialogVoiceOnboarding
            ctx={ctx}
            directory={session.location.directory}
            onStart={() => {
              setOnboarding("done", true)
              void start(server, ctx, session)
            }}
          />
        )),
      )

    return {
      state,
      meter,
      end,
      toggleTranscript: () => setState("transcript", (open) => !open),
      mute: () => current.call?.mute(),
      rejoin: () => current.call?.join(),
      view(server: ServerConnection.Key, session: SessionInfo) {
        if (state.server !== server) return
        target.session = session
        current.call?.view(session)
      },
      start,
      begin(server: ServerConnection.Key, ctx: ServerCtx, session: SessionInfo) {
        if (onboarding.done) return void start(server, ctx, session)
        onboard(server, ctx, session)
      },
      setup() {
        const server = target.server
        const ctx = target.ctx
        const session = target.session

        if (!server || !ctx || !session) return
        void end().then(() => onboard(server, ctx, session))
      },
    }
  },
})
