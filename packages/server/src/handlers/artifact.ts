import { Artifact } from "@opencode/core/artifact"
import { KV } from "@opencode/core/kv"
import { Location } from "@opencode/core/location"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import {
  ArtifactNotFoundError,
  ConflictError,
  FileNotFoundError,
  ForbiddenError,
  InvalidRequestError,
  ProviderNotFoundError,
  ServiceUnavailableError,
} from "@opencode/protocol/errors"
import { Effect, Option, Schema } from "effect"
import { type HttpServerRequest, HttpServerResponse } from "effect/http"
import { HttpApiSchema } from "effect/http-api"
import { awaitPlugins, locationErrors, requestRef, response } from "../location"
import { missingSession } from "./session-error"

const StoredSessionRef = Schema.Struct({
  sessionID: Schema.optional(Session.ID),
})
const decodeStoredSessionRef = Schema.decodeUnknownOption(StoredSessionRef)

/** `artifacts.*` in the `server.experimental` group. Every route needs experimental.artifacts in the resolved Location. */
export const makeArtifact = Effect.gen(function* () {
  const locations = yield* LocationServiceMap.Service
  const sessions = yield* Session.Service
  const kv = yield* KV.Service

  const resolveLocation = (
    request: HttpServerRequest.HttpServerRequest,
    options: {
      readonly location?: { readonly directory?: string; readonly workspace?: string }
      readonly sessionID?: Session.ID
      readonly artifactID?: Artifact.ID
    },
  ) =>
    Effect.gen(function* () {
      if (options.location?.directory !== undefined) {
        return Location.Ref.make({ directory: AbsolutePath.make(options.location.directory) })
      }
      if (options.sessionID !== undefined) {
        const session = yield* sessions.get(options.sessionID).pipe(Effect.mapError(missingSession))
        return session.location
      }
      if (options.artifactID !== undefined) {
        const raw = yield* kv.get(`artifact:item:${options.artifactID}`)
        const parsed = raw === undefined ? Option.none() : decodeStoredSessionRef(raw)
        const linkedSessionID = Option.isSome(parsed) ? parsed.value.sessionID : undefined
        if (linkedSessionID !== undefined) {
          const found = yield* sessions.get(linkedSessionID).pipe(Effect.option)
          if (Option.isSome(found)) return found.value.location
        }
      }
      return requestRef(request)
    })

  const inLocation = <A, E>(
    request: HttpServerRequest.HttpServerRequest,
    options: {
      readonly location?: { readonly directory?: string; readonly workspace?: string }
      readonly sessionID?: Session.ID
      readonly artifactID?: Artifact.ID
    },
    effect: Effect.Effect<A, E, Artifact.Service | Location.Service>,
  ) =>
    Effect.gen(function* () {
      const ref = yield* resolveLocation(request, options)
      return yield* awaitPlugins.pipe(Effect.andThen(effect), Effect.provide(locations.get(ref)), locationErrors)
    })

  const mapCommon = <A, E>(effect: Effect.Effect<A, E>) =>
    effect.pipe(
      Effect.catchTags({
        "Artifact.DisabledError": (error: Artifact.DisabledError) =>
          Effect.fail(new ForbiddenError({ message: error.message })),
        "Artifact.NotFoundError": (error: Artifact.NotFoundError) =>
          Effect.fail(new ArtifactNotFoundError({ artifactID: error.artifactID, message: error.message })),
        "Artifact.ProviderNotFoundError": (error: Artifact.ProviderNotFoundError) =>
          Effect.fail(new ProviderNotFoundError({ providerID: error.providerID, message: error.message })),
        "Artifact.InvalidError": (error: Artifact.InvalidError) =>
          Effect.fail(
            new InvalidRequestError({ message: error.message, kind: "artifact_invalid", field: error.field }),
          ),
        "Artifact.ConflictError": (error: Artifact.ConflictError) =>
          Effect.fail(new ConflictError({ message: error.message, resource: error.artifactID })),
        "Artifact.ProviderError": (error: Artifact.ProviderError) =>
          Effect.fail(new ServiceUnavailableError({ message: error.message, service: error.providerID })),
        "Session.NotFoundError": (error: Session.NotFoundError) => Effect.fail(missingSession(error)),
      } as never),
    )

  return {
    providers: (
      query: { readonly location?: { readonly directory?: string } },
      request: HttpServerRequest.HttpServerRequest,
    ) =>
      inLocation(
        request,
        { location: query.location },
        Effect.gen(function* () {
          const artifacts = yield* Artifact.Service
          return yield* response(artifacts.providers())
        }),
      ).pipe(mapCommon),

    list: (
      query: {
        readonly location?: { readonly directory?: string }
        readonly sessionID?: Session.ID
        readonly provider?: Artifact.ProviderID
      },
      request: HttpServerRequest.HttpServerRequest,
    ) =>
      inLocation(
        request,
        { location: query.location, sessionID: query.sessionID },
        Effect.gen(function* () {
          const artifacts = yield* Artifact.Service
          return yield* response(artifacts.list({ sessionID: query.sessionID, provider: query.provider }))
        }),
      ).pipe(mapCommon),

    create: (
      query: { readonly location?: { readonly directory?: string } },
      payload: Artifact.CreateInput,
      request: HttpServerRequest.HttpServerRequest,
    ) =>
      inLocation(
        request,
        { location: query.location, sessionID: payload.sessionID },
        Effect.gen(function* () {
          const artifacts = yield* Artifact.Service
          return yield* response(artifacts.create(payload))
        }),
      ).pipe(mapCommon),

    get: (
      artifactID: Artifact.ID,
      query: { readonly location?: { readonly directory?: string } },
      request: HttpServerRequest.HttpServerRequest,
    ) =>
      inLocation(
        request,
        { location: query.location, artifactID },
        Effect.gen(function* () {
          const artifacts = yield* Artifact.Service
          return yield* response(artifacts.get(artifactID))
        }),
      ).pipe(mapCommon),

    update: (
      artifactID: Artifact.ID,
      query: { readonly location?: { readonly directory?: string } },
      payload: Artifact.UpdateInput,
      request: HttpServerRequest.HttpServerRequest,
    ) =>
      inLocation(
        request,
        { location: query.location, sessionID: payload.sessionID, artifactID },
        Effect.gen(function* () {
          const artifacts = yield* Artifact.Service
          return yield* response(artifacts.update(artifactID, payload))
        }),
      ).pipe(mapCommon),

    remove: (
      artifactID: Artifact.ID,
      query: { readonly location?: { readonly directory?: string } },
      request: HttpServerRequest.HttpServerRequest,
    ) =>
      inLocation(
        request,
        { location: query.location, artifactID },
        Effect.gen(function* () {
          const artifacts = yield* Artifact.Service
          yield* artifacts.remove(artifactID)
          return HttpApiSchema.NoContent.make()
        }),
      ).pipe(mapCommon),

    view: (
      artifactID: Artifact.ID,
      query: { readonly location?: { readonly directory?: string }; readonly version?: number; readonly raw?: string },
      request: HttpServerRequest.HttpServerRequest,
    ) =>
      inLocation(
        request,
        { location: query.location, artifactID },
        Effect.gen(function* () {
          const artifacts = yield* Artifact.Service
          const parsed = new URL(request.url, "http://localhost")
          const prefix = `/api/experimental/artifacts/${encodeURIComponent(artifactID)}/view`
          if (parsed.pathname === prefix) {
            yield* artifacts.info(artifactID)
            return HttpServerResponse.empty({
              status: 308,
              headers: { location: `${prefix}/${parsed.search}` },
            })
          }
          const subpath = extractViewSubpath(request.url, artifactID)
          const file = yield* artifacts
            .readLocalFile(artifactID, {
              path: subpath,
              version: query.version,
              raw: query.raw === "1" || query.raw === "true",
            })
            .pipe(
              Effect.catchTag("Artifact.FileMissingError", (error) =>
                Effect.fail(new FileNotFoundError({ path: error.path, message: error.message })),
              ),
            )
          return HttpServerResponse.uint8Array(file.bytes, {
            status: 200,
            contentType: file.mime,
            headers: {
              "cache-control": query.version === undefined ? "private, no-cache" : "private, max-age=31536000, immutable",
              "x-content-type-options": "nosniff",
            },
          })
        }),
      ).pipe(mapCommon),
  }
})

function extractViewSubpath(url: string, artifactID: Artifact.ID) {
  const pathname = new URL(url, "http://localhost").pathname
  const prefix = `/api/experimental/artifacts/${encodeURIComponent(artifactID)}/view/`
  if (!pathname.startsWith(prefix)) return undefined
  const raw = pathname.slice(prefix.length)
  if (!raw) return undefined
  try {
    return decodeURIComponent(raw)
  } catch {
    return raw
  }
}
