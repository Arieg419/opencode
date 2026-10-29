export * as ConfigNetwork from "./network.js"

import { Schema } from "effect"
import { optional } from "../schema.js"

const NonEmpty = Schema.Trim.check(Schema.isNonEmpty())

export class ClientCertificate extends Schema.Class<ClientCertificate>("Config.Network.ClientCertificate")({
  hosts: NonEmpty.pipe(Schema.Array, Schema.check(Schema.isMinLength(1))).annotate({
    description: "Hostnames that receive this certificate. `*` and `?` wildcards match any characters",
  }),
  cert: NonEmpty.annotate({
    description: "Path to the PEM client certificate, absolute, `~/`, or relative to this configuration file",
  }),
  key: NonEmpty.annotate({
    description: "Path to the PEM private key, absolute, `~/`, or relative to this configuration file",
  }),
  passphrase: Schema.String.pipe(optional).annotate({
    description: "Passphrase for an encrypted private key",
  }),
}) {}

export class Info extends Schema.Class<Info>("Config.Network")({
  client_certificates: ClientCertificate.pipe(Schema.Array, optional).annotate({
    description:
      "Client certificates presented to matching hosts for mutual TLS. Read from system, managed, and global configuration only",
  }),
}) {}
