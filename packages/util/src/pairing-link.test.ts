import { describe, expect, test } from "bun:test"
import { pairPageLink, pairPagePayload, reachableURLs } from "./pairing-link.js"

describe("pair page link", () => {
  const payload = { code: "abc_DEF-123", urls: ["http://192.168.1.2:49374", "https://r1.example.opentunnel.xyz"] }

  test("carries the code and addresses as base64url JSON in the fragment", () => {
    const link = pairPageLink(payload)
    const fragment = new URL(link).hash.slice(1)

    expect(link.startsWith("https://opencode.ai/pair#")).toBe(true)
    expect(fragment).toMatch(/^[A-Za-z0-9_-]+$/)
    expect(JSON.parse(Buffer.from(fragment, "base64url").toString("utf8"))).toEqual(payload)
  })

  test("round trips", () => {
    expect(JSON.parse(pairPagePayload(` ${pairPageLink(payload)} `) ?? "")).toEqual(payload)
  })

  test("reads padded fragments and a trailing slash", () => {
    const fragment = Buffer.from(JSON.stringify(payload)).toString("base64").replaceAll("+", "-").replaceAll("/", "_")

    expect(pairPagePayload(`https://opencode.ai/pair/#${fragment}`)).toBe(JSON.stringify(payload))
  })

  test("keeps only addresses another device can reach", () => {
    expect(
      reachableURLs([
        "http://127.0.0.1:4096",
        "http://localhost:4096",
        "http://box.localhost:4096",
        "http://[::1]:4096",
        "http://192.168.1.2:4096",
        "http://100.64.0.3:4096",
        "https://r1.example.opentunnel.xyz",
        "not a url",
      ]),
    ).toEqual(["http://192.168.1.2:4096", "http://100.64.0.3:4096", "https://r1.example.opentunnel.xyz"])
  })

  test("ignores other links", () => {
    const fragment = new URL(pairPageLink(payload)).hash

    expect(pairPagePayload(`http://opencode.ai/pair${fragment}`)).toBeUndefined()
    expect(pairPagePayload(`https://example.com/pair${fragment}`)).toBeUndefined()
    expect(pairPagePayload(`https://opencode.ai/pairing${fragment}`)).toBeUndefined()
    expect(pairPagePayload(`https://opencode.ai/pair?x=1${fragment}`)).toBeUndefined()
    expect(pairPagePayload("https://opencode.ai/pair")).toBeUndefined()
    expect(pairPagePayload("https://opencode.ai/pair#not base64!")).toBeUndefined()
    expect(pairPagePayload("http://192.168.1.2:49374/auth/connect/abc")).toBeUndefined()
    expect(pairPagePayload("not a url")).toBeUndefined()
  })
})
