import type { OpenCodeClient } from "@opencode/client"
import type { Artifact } from "@opencode/schema/artifact"
import type { Session } from "@opencode/schema/session"
import type { Transform } from "./registration.js"

export interface ArtifactProviderCreateInput {
  readonly id: Artifact.ID
  readonly title: string
  readonly entry: string
  readonly version: number
  readonly files: Readonly<Record<string, Uint8Array>>
  readonly sessionID?: Session.ID
  readonly metadata?: Readonly<Record<string, string>>
}

export interface ArtifactProviderUpdateInput {
  readonly title: string
  readonly entry: string
  readonly version: number
  readonly files: Readonly<Record<string, Uint8Array>>
  readonly sessionID?: Session.ID
  readonly metadata?: Readonly<Record<string, string>>
}

export interface ArtifactProviderResult {
  readonly url?: string
}

export interface ArtifactProviderDefinition {
  readonly id: string
  readonly name: string
  readonly capabilities: readonly Artifact.Capability[]
  readonly prompt: string
  readonly create: (
    input: ArtifactProviderCreateInput,
    context: { readonly signal: AbortSignal },
  ) => Promise<ArtifactProviderResult>
  readonly read: (
    id: Artifact.ID,
    context: { readonly signal: AbortSignal },
  ) => Promise<Readonly<Record<string, Uint8Array>>>
  readonly update: (
    id: Artifact.ID,
    input: ArtifactProviderUpdateInput,
    context: { readonly signal: AbortSignal },
  ) => Promise<ArtifactProviderResult>
  readonly delete: (id: Artifact.ID, context: { readonly signal: AbortSignal }) => Promise<void>
}

export interface ArtifactEditor {
  add(definition: ArtifactProviderDefinition): void
  remove(id: string): void
  readonly default: {
    get(): string | undefined
    set(providerID: string | undefined): void
  }
}

type PromiseArtifactMethods = Omit<OpenCodeClient["experimental"]["artifacts"], "view">

export interface ExperimentalArtifactDomain extends PromiseArtifactMethods {
  readonly transform: Transform<ArtifactEditor>
  readonly reload: () => Promise<void>
}
