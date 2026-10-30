export * as ConfigProviderPlugin from "./provider.js"

import type { ModelEditor } from "@opencode/plugin/effect/model"
import type { ProviderEditor, ProviderRecord } from "@opencode/plugin/effect/provider"
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
      for (const [integrationID, provider] of configuredProviders(loaded.entries)) {
        if (!integrations.get(integrationID))
          integrations.method.update({ integrationID, method: { type: "key", label: "Manually enter API Key" } })
        integrations.update(integrationID, (integration) => {
          integration.name = provider.name || integration.name
        })
        if (provider.env !== undefined)
          integrations.method.update({ integrationID, method: { type: "env", names: [...provider.env] } })
      }
    })

    const sources = {
      defaultModel: undefined as Document["info"]["model"],
      configured: [] as readonly { providerID: string; id: string; configs: readonly ModelConfig[] }[],
    }
    yield* ctx.provider.transform((providers) => {
      const snapshots = new Map<string, Pick<ProviderRecord, "provider" | "models"> | undefined>()
      const origins = new Map<string, Pick<ProviderRecord, "provider" | "models"> | undefined>()
      const configured = new Map<string, { providerID: string; id: string; base?: Model.Info; configs: ModelConfig[] }>()
      const snapshot = (id: string) => {
        if (!snapshots.has(id)) {
          const existing = providers.get(id)
          snapshots.set(id, existing && structuredClone({ provider: existing.provider, models: existing.models }))
        }
        return snapshots.get(id)
      }

      for (const [providerID, item] of configuredProviders(loaded.entries)) {
        if (!origins.has(providerID)) origins.set(providerID, snapshot(providerID))
        const current = providers.get(providerID)
        const sourceOrigin = snapshot(item.canonical ?? current?.provider.canonical ?? providerID)
        const { changed } = overlayProvider(providers, providerID, item)
        if (changed) {
          origins.set(providerID, sourceOrigin)
          for (const [key, entry] of configured) if (entry.providerID === providerID) configured.delete(key)
        }
        const origin = origins.get(providerID)
        const updated = providers.get(providerID)?.provider
        const clearPackage = item.package !== undefined || updated?.package !== (origin?.provider.package ?? "")
        const clearBaseURL =
          item.settings?.baseURL !== undefined || updated?.settings?.baseURL !== origin?.provider.settings?.baseURL
        // A configured provider baseURL replaces every catalog per-model endpoint, not only those of configured models.
        if (item.settings?.baseURL !== undefined)
          for (const [id, model] of providers.get(providerID)?.models ?? [])
            if (model.settings?.baseURL !== undefined)
              providers.models.update(providerID, id, (draft) => {
                delete draft.settings?.baseURL
              })

        for (const [id, config] of Object.entries(item.models ?? {})) {
          const base = sourceOrigin?.models.get(config.modelID ?? id) ?? sourceOrigin?.models.get(id)
          const inherit = changed || !current?.models.has(id)
          const key = `${providerID}\0${id}`
          const existing = !inherit ? configured.get(key) : undefined
          if (existing) existing.configs.push(config)
          if (!existing) configured.set(key, { providerID, id, base, configs: [config] })
          overlayFields(providers.models, providerID, id, config, inherit ? { base, clearPackage, clearBaseURL } : undefined)
        }
      }

      for (const [providerID, origin] of origins) {
        const record = providers.get(providerID)
        if (!record) continue
        for (const [id, model] of record.models) {
          const entry = configured.get(`${providerID}\0${id}`)
          const base = entry ? entry.base : origin?.models.get(id)
          const finalPackage = model.package ?? record.provider.package
          const hasExplicitVariants = entry?.configs.some(
            (config) => config.capabilities?.reasoning !== undefined || config.variants !== undefined,
          )
          const repackaged =
            base !== undefined &&
            base.variants.length > 0 &&
            finalPackage !== (base.package ?? origin?.provider.package) &&
            Variant.canResolve(finalPackage)
          if (repackaged || (entry !== undefined && !base && !hasExplicitVariants))
            providers.models.update(providerID, id, (draft) => {
              draft.variants = [...Variant.resolve({ ...draft, package: finalPackage })]
            })
          for (const config of entry?.configs ?? [])
            applyExplicitVariants(providers.models, record.provider.package, providerID, id, config)
        }
      }

      sources.defaultModel = Config.latest(loaded.entries, "model")
      sources.configured = Array.from(configured.values())
    })

    // Re-apply surviving explicit config fields after external model transforms so configured values win
    // while leaving plugin-supplied fields (including custom model variants) intact when omitted in config.
    yield* ctx.model.transform((models) => {
      if (sources.defaultModel !== undefined)
        models.default.set(sources.defaultModel.providerID, sources.defaultModel.model)
      for (const entry of sources.configured) {
        const seeded = models.provider.get(entry.providerID)
        if (!models.get(entry.providerID, entry.id)) {
          const base = seeded?.models.get(entry.id)
          if (base) models.update(entry.providerID, entry.id, (model) => Object.assign(model, structuredClone(base)))
        }
        for (const config of entry.configs) {
          overlayFields(models, entry.providerID, entry.id, config)
          applyExplicitVariants(models, seeded?.provider.package, entry.providerID, entry.id, config)
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

export function integrate(providers: ProviderEditor, document: ConfigIntegration.Document) {
  for (const [providerID, item] of Object.entries(document.info.providers ?? {})) {
    const { source } = overlayProvider(providers, providerID, item, document.integrationID)
    const providerPackage = providers.get(providerID)?.provider.package
    for (const [id, config] of Object.entries(item.models ?? {})) {
      const base = source?.models.get(config.modelID ?? id) ?? source?.models.get(id)
      overlayFields(providers.models, providerID, id, config, {
        base,
        clearPackage: base !== undefined && item.package !== undefined,
        clearBaseURL: base !== undefined && item.settings?.baseURL !== undefined,
      })
      applyExplicitVariants(providers.models, providerPackage, providerID, id, config)
    }
    const configured = providers.get(providerID)
    if (configured)
      providers.add({
        info: configured.provider,
        models: Array.from(configured.models.values()),
        sourceConnection: document.connection,
      })
  }
}

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

function overlayFields(
  models: Pick<ModelEditor, "update">,
  providerID: string,
  id: string,
  config: ModelConfig,
  reset?: { readonly base: Model.Info | undefined; readonly clearPackage: boolean; readonly clearBaseURL: boolean },
) {
  models.update(providerID, id, (model) => {
    if (reset) {
      if (reset.base) Object.assign(model, structuredClone(reset.base))
      if (reset.clearPackage) model.package = undefined
      if (reset.clearBaseURL && model.settings) delete model.settings.baseURL
    }
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
    if (config.cost !== undefined)
      model.cost = (Array.isArray(config.cost) ? config.cost : [config.cost]).map((cost) => ({
        tier: cost.tier && { ...cost.tier },
        input: cost.input,
        output: cost.output,
        cache: {
          read: cost.cache?.read ?? Money.USDPerMillionTokens.zero,
          write: cost.cache?.write ?? Money.USDPerMillionTokens.zero,
        },
      }))
    if (config.disabled !== undefined) model.enabled = !config.disabled
  })
}

function applyExplicitVariants(
  models: Pick<ModelEditor, "update">,
  providerPackage: string | undefined,
  providerID: string,
  id: string,
  config: ModelConfig,
) {
  if (config.capabilities?.reasoning === undefined && config.variants === undefined) return
  models.update(providerID, id, (model) => {
    if (config.capabilities?.reasoning !== undefined)
      model.variants = [
        ...Variant.resolve(
          { ...model, package: model.package ?? providerPackage },
          Variant.supports(config.capabilities.reasoning),
        ),
      ]
    if (config.variants !== undefined) {
      model.variants ??= []
      for (const variant of config.variants) {
        const found = model.variants.find((item) => item.id === variant.id)
        const target = found ?? { id: variant.id }
        if (!found) model.variants.push(target)
        if (variant.settings !== undefined) target.settings = Provider.mergeOverlay(target.settings, variant.settings)
        if (variant.headers !== undefined) target.headers = Provider.mergeHeaders(target.headers, variant.headers)
        if (variant.body !== undefined) target.body = Provider.mergeOverlay(target.body, variant.body)
      }
    }
  })
}
