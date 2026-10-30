export * as ConfigProviderPlugin from "./provider.js"

import type { ModelEditor } from "@opencode/plugin/effect/model"
import type { ProviderEditor } from "@opencode/plugin/effect/provider"
import { define } from "@opencode/plugin/effect/plugin"
import { Document, type Entry } from "@opencode/schema/config"
import { ConfigProvider } from "@opencode/schema/config/provider"
import { Money } from "@opencode/schema/money"
import { Effect } from "effect"
import { Config } from "../../config.js"
import { Model } from "../../model.js"
import { Provider } from "../../provider.js"
import { Variant } from "../../variant.js"
import { ConfigIntegration } from "../integration.js"
import { ConfigManaged } from "../managed.js"
import { ConfigEntryObserver } from "./entry-observer.js"

type ModelConfig = NonNullable<ConfigProvider.Info["models"]>[string]

type OriginProvider = {
  readonly package: string
  readonly models: ReadonlyMap<string, Model.Info>
}

type Definition = {
  readonly providerID: string
  readonly clearPackage: boolean
  readonly clearBaseURL: boolean
  readonly models: ReadonlyMap<string, { readonly inherit: boolean; readonly base?: Model.Info }>
}

type TouchedModel = {
  readonly providerID: string
  readonly id: string
  base: Model.Info | undefined
  readonly hadVariants: boolean
  packageConfigured: boolean
  reasoning?: ConfigProvider.Reasoning
  readonly variants: NonNullable<ModelConfig["variants"]>[]
}

export const Plugin = define({
  id: "opencode.config.provider",
  effect: Effect.fn(function* (ctx) {
    const config = yield* Config.Service
    const loaded = yield* ConfigEntryObserver.observe(
      config,
      ctx.event,
      ctx.integration.reload().pipe(Effect.andThen(ctx.provider.reload())),
    )
    yield* ctx.integration.transform((integrations) => {
      for (const [id, provider] of configuredProviders(loaded.entries)) {
        const integrationID = id
        if (!integrations.get(integrationID)) {
          integrations.method.update({
            integrationID,
            method: { type: "key", label: "Manually enter API Key" },
          })
        }
        integrations.update(integrationID, (integration) => {
          integration.name = provider.name || integration.name
        })
        if (provider.env !== undefined) {
          integrations.method.update({
            integrationID,
            method: { type: "env", names: [...provider.env] },
          })
        }
      }
    })

    const sources = {
      defaultModel: undefined as Document["info"]["model"],
      providerOrigins: new Map<string, OriginProvider | undefined>(),
      models: new Map<ConfigProvider.Info, Definition>(),
    }
    yield* ctx.provider.transform((providers) => {
      const origins = new Map<string, OriginProvider | undefined>()
      const providerOrigins = new Map<string, OriginProvider | undefined>()
      const providerFlags = new Map<string, { clearPackage: boolean; clearBaseURL: boolean }>()
      const next = new Map<ConfigProvider.Info, Definition>()
      const snapshot = (target: string) => {
        if (!origins.has(target)) {
          const existing = providers.get(target)
          origins.set(target, existing && { package: existing.provider.package, models: new Map(existing.models) })
        }
        return origins.get(target)
      }

      for (const [providerID, item] of configuredProviders(loaded.entries)) {
        snapshot(providerID)
        const current = providers.get(providerID)
        const sourceID = item.canonical ?? current?.provider.canonical ?? providerID
        const origin = snapshot(sourceID)
        const { changed } = overlayProvider(providers, providerID, item)
        if (!providerOrigins.has(providerID) || changed) providerOrigins.set(providerID, origin)

        const previousFlags = providerFlags.get(providerID)
        const flags = {
          clearPackage: (previousFlags?.clearPackage && !changed) || item.package !== undefined,
          clearBaseURL: (previousFlags?.clearBaseURL && !changed) || item.settings?.baseURL !== undefined,
        }
        providerFlags.set(providerID, flags)

        const definitions = new Map<string, { readonly inherit: boolean; readonly base?: Model.Info }>()
        for (const [id, config] of Object.entries(item.models ?? {})) {
          const base = origin?.models.get(config.modelID ?? id) ?? origin?.models.get(id)
          const inherit = changed || !current?.models.has(id)
          // Bind the pre-config source at this point in the provider fold. Later source edits/removal
          // and its credential availability must not change an already-defined alias.
          definitions.set(id, { inherit, base: base && structuredClone(base) })
          if (!inherit) continue
          providers.models.update(providerID, id, (model) => {
            if (base) Object.assign(model, structuredClone(base))
            if (flags.clearPackage) model.package = undefined
          })
        }
        // A configured provider baseURL replaces every catalog per-model endpoint, not only those of configured
        // models. Configured per-model settings are applied later and still win.
        if (item.settings?.baseURL !== undefined)
          for (const [id, model] of providers.get(providerID)?.models ?? [])
            if (model.settings?.baseURL !== undefined)
              providers.models.update(providerID, id, (model) => {
                delete model.settings?.baseURL
              })
        next.set(item, {
          providerID,
          clearPackage: flags.clearPackage,
          clearBaseURL: flags.clearBaseURL,
          models: definitions,
        })
      }
      for (const [item, definition] of next) {
        for (const [id, config] of Object.entries(item.models ?? {})) {
          if (config.limit === undefined) continue
          providers.models.update(definition.providerID, id, (model) => {
            model.limit = { ...model.limit, ...config.limit }
          })
        }
      }
      sources.defaultModel = Config.latest(loaded.entries, "model")
      sources.providerOrigins = providerOrigins
      sources.models = next
    })

    // Keep explicit model overrides in their late registration position, after external
    // model transforms. Apply all documents' model fields first so variants resolve once
    // against the final package, modelID, limit, and reasoning capabilities.
    yield* ctx.model.transform((models) => {
      const configuredDefault = sources.defaultModel
      if (configuredDefault !== undefined) models.default.set(configuredDefault.providerID, configuredDefault.model)

      const touched = new Map<string, TouchedModel>()
      for (const [item, definition] of sources.models) {
        const providerID = definition.providerID
        for (const [id, config] of Object.entries(item.models ?? {})) {
          const source = definition.models.get(id)
          const key = `${providerID}\0${id}`
          const existing = models.get(providerID, id)
          const current = touched.get(key)
          const state = current ?? {
            providerID,
            id,
            base: source?.base,
            hadVariants: (existing?.variants.length ?? 0) > 0,
            packageConfigured: false,
            variants: [],
          }
          if (!current) touched.set(key, state)
          if (current && source?.inherit) {
            state.base = source.base
            state.packageConfigured = false
            state.reasoning = undefined
            state.variants.length = 0
          }
          const inherit = source?.inherit || !existing
          overlayFields(models, providerID, id, config, {
            base: inherit ? source?.base : undefined,
            clearPackage: inherit && (definition.clearPackage || item.package !== undefined),
            clearBaseURL: inherit && (definition.clearBaseURL || item.settings?.baseURL !== undefined),
          })
          if (definition.clearPackage || item.package !== undefined || config.package !== undefined)
            state.packageConfigured = true
          if (config.capabilities?.reasoning !== undefined) state.reasoning = config.capabilities.reasoning
          if (config.variants !== undefined) state.variants.push(config.variants)
        }
      }

      for (const [providerID, origin] of sources.providerOrigins) {
        const providerPackage = models.provider.get(providerID)?.provider.package
        if (!origin?.package || providerPackage === origin.package || !Variant.canResolve(providerPackage)) continue
        for (const model of models.list(providerID)) {
          const base = origin.models.get(model.id)
          if (!base || model.package !== undefined || base.variants.length === 0) continue
          if (touched.has(`${providerID}\0${model.id}`)) continue
          models.update(providerID, model.id, (draft) => {
            draft.variants = [...Variant.resolve({ ...draft, package: providerPackage })]
          })
        }
      }

      for (const state of touched.values()) {
        reconcileVariants(models, state, sources.providerOrigins.get(state.providerID))
        for (const list of state.variants) {
          models.update(state.providerID, state.id, (model) => {
            overlayVariants(model, list)
          })
        }
      }
    })
  }),
})

// Integration documents are left out: their providers authenticate with the integration's connection, so they add no
// integration or method, and the integration's own plugin applies them earlier in the provider chain (`integrate`).
// A provider the Console supplies is shaped only by the Console and managed configuration: user, project, and remote
// documents cannot override any part of it, since it carries the org's token and a repository must not redirect it.
function configuredProviders(entries: readonly Entry[]) {
  const supplied = new Set(
    entries
      .filter((entry): entry is ConfigIntegration.Document => entry instanceof ConfigIntegration.Document)
      .flatMap((document) => Object.keys(document.info.providers ?? {})),
  )
  return entries
    .filter((entry): entry is Document => entry.type === "document" && !(entry instanceof ConfigIntegration.Document))
    .flatMap((file) =>
      Object.entries(file.info.providers ?? {}).filter(
        ([id]) => !supplied.has(id) || file instanceof ConfigManaged.Document,
      ),
    )
}

/**
 * Applies an integration document's providers. The integration's plugin calls this at its own position in the
 * provider chain, so provider plugins registered after it treat these providers as configured ones rather than
 * as their canonical provider, while user and project config still overlay them in this plugin's late transform.
 */
export function integrate(providers: ProviderEditor, document: ConfigIntegration.Document) {
  for (const [providerID, item] of Object.entries(document.info.providers ?? {})) {
    const { source } = overlayProvider(providers, providerID, item, document.integrationID)
    const providerPackage = providers.get(providerID)?.provider.package
    // An integration lists the models it routes: each starts again from the catalog and takes the provider's
    // package and URL. Its models are complete here, so later model transforms and config apply on top.
    for (const [id, config] of Object.entries(item.models ?? {})) {
      const base = source?.models.get(config.modelID ?? id) ?? source?.models.get(id)
      overlayFields(providers.models, providerID, id, config, {
        base,
        clearPackage: base !== undefined && item.package !== undefined,
        clearBaseURL: base !== undefined && item.settings?.baseURL !== undefined,
      })
      if (config.capabilities?.reasoning !== undefined || config.variants !== undefined)
        providers.models.update(providerID, id, (model) => {
          if (config.capabilities?.reasoning !== undefined)
            model.variants = [
              ...Variant.resolve(
                { ...model, package: model.package ?? providerPackage },
                Variant.supports(config.capabilities.reasoning),
              ),
            ]
          if (config.variants !== undefined) overlayVariants(model, config.variants)
        })
    }
    const configured = providers.get(providerID)
    // Bound to the connection so its providers disappear as soon as the account changes or signs out.
    if (configured)
      providers.add({
        info: configured.provider,
        models: Array.from(configured.models.values()),
        sourceConnection: document.connection,
      })
  }
}

function reconcileVariants(models: ModelEditor, state: TouchedModel, origin: OriginProvider | undefined) {
  const providerPackage = models.provider.get(state.providerID)?.provider.package
  const finalPackage = models.get(state.providerID, state.id)?.package ?? providerPackage
  if (state.reasoning !== undefined) {
    models.update(state.providerID, state.id, (model) => {
      model.variants = [...Variant.resolve({ ...model, package: finalPackage }, Variant.supports(state.reasoning!))]
    })
    return
  }

  const configChangedPackage =
    state.packageConfigured || (Boolean(origin?.package) && providerPackage !== origin?.package)
  const repackaged =
    configChangedPackage &&
    state.base !== undefined &&
    state.base.variants.length > 0 &&
    finalPackage !== (state.base.package ?? origin?.package) &&
    Variant.canResolve(finalPackage)
  if (repackaged) {
    models.update(state.providerID, state.id, (model) => {
      model.variants = [...Variant.resolve({ ...model, package: finalPackage })]
    })
    return
  }

  // Custom models start without variants, so non-empty variants here came from an earlier transform.
  if (!state.base && !state.hadVariants && state.variants.length === 0)
    models.update(state.providerID, state.id, (model) => {
      model.variants = [...Variant.resolve({ ...model, package: finalPackage })]
    })
}

/** Applies one configured provider, first copying its source when `canonical` points it at another provider. */
function overlayProvider(
  providers: ProviderEditor,
  providerID: string,
  item: ConfigProvider.Info,
  integrationID?: ConfigIntegration.Document["integrationID"],
) {
  const current = providers.get(providerID)
  const source = providers.get(item.canonical ?? current?.provider.canonical ?? providerID)
  const changed = item.canonical !== undefined && item.canonical !== current?.provider.canonical
  providers.update(providerID, (provider) => {
    if (changed && source && source.provider !== provider)
      Object.assign(provider, structuredClone(source.provider), {
        id: provider.id,
        integrationID: provider.integrationID,
      })
    if (integrationID) provider.integrationID = integrationID
    provider.activation = "enabled"
    if (item.canonical !== undefined) provider.canonical = item.canonical
    if (item.name) provider.name = item.name
    if (item.package !== undefined) provider.package = item.package
    if (item.compatibility !== undefined) provider.compatibility = { ...provider.compatibility, ...item.compatibility }
    if (item.settings !== undefined) provider.settings = Provider.mergeOverlay(provider.settings, item.settings)
    if (item.headers !== undefined) provider.headers = Provider.mergeHeaders(provider.headers, item.headers)
    if (item.body !== undefined) provider.body = Provider.mergeOverlay(provider.body, item.body)
  })
  return { current, source, changed }
}

/** Applies non-variant model fields in a single edit, resetting to `reset.base` when restarting from the catalog. */
function overlayFields(
  models: Pick<ModelEditor, "update">,
  providerID: string,
  id: string,
  config: ModelConfig,
  reset: { readonly base: Model.Info | undefined; readonly clearPackage: boolean; readonly clearBaseURL: boolean },
) {
  models.update(providerID, id, (model) => {
    if (reset.base) Object.assign(model, structuredClone(reset.base))
    if (reset.clearPackage) model.package = undefined
    if (reset.clearBaseURL && model.settings) delete model.settings.baseURL
    if (config.type !== undefined) model.type = config.type
    if (config.family !== undefined) model.family = config.family
    if (config.name) model.name = config.name
    if (config.modelID !== undefined) model.modelID = config.modelID
    if (config.releaseDate !== undefined) model.time = { released: Date.parse(config.releaseDate) }
    if (config.compatibility !== undefined) model.compatibility = { ...model.compatibility, ...config.compatibility }
    if (config.package !== undefined) model.package = config.package
    if (config.settings !== undefined) model.settings = Provider.mergeOverlay(model.settings, config.settings)
    if (config.headers !== undefined) model.headers = Provider.mergeHeaders(model.headers, config.headers)
    if (config.body !== undefined) model.body = Provider.mergeOverlay(model.body, config.body)
    if (config.capabilities !== undefined)
      model.capabilities = Model.mergeCapabilities(config.capabilities, model.capabilities)
    if (config.limit !== undefined) model.limit = { ...model.limit, ...config.limit }
    if (config.cost !== undefined) {
      model.cost = (Array.isArray(config.cost) ? config.cost : [config.cost]).map((cost) => ({
        tier: cost.tier && { ...cost.tier },
        input: cost.input,
        output: cost.output,
        cache: {
          read: cost.cache?.read ?? Money.USDPerMillionTokens.zero,
          write: cost.cache?.write ?? Money.USDPerMillionTokens.zero,
        },
      }))
    }
    if (config.disabled !== undefined) model.enabled = !config.disabled
  })
}

function overlayVariants(
  model: { variants?: Model.MutableInfo["variants"] },
  variants: NonNullable<ModelConfig["variants"]>,
) {
  model.variants ??= []
  for (const variant of variants) {
    const found = model.variants.find((item) => item.id === variant.id)
    const target = found ?? { id: variant.id }
    if (!found) model.variants.push(target)
    if (variant.settings !== undefined) target.settings = Provider.mergeOverlay(target.settings, variant.settings)
    if (variant.headers !== undefined) target.headers = Provider.mergeHeaders(target.headers, variant.headers)
    if (variant.body !== undefined) target.body = Provider.mergeOverlay(target.body, variant.body)
  }
}
