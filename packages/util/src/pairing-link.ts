import { base64ToBytes, bytesToBase64 } from "./base64.js"

// Pairing QR codes open this page. opencode.ai serves an apple-app-site-association file that lets the OpenCode iOS
// app claim it, so any camera app opens the code in the app. The single-use code and the server addresses ride in the
// fragment as base64url {"code","urls"} JSON, which a browser never sends to a server.
const PAGE = "https://opencode.ai/pair"

/** The https link a pairing QR code carries. */
export function pairPageLink(input: { readonly code: string; readonly urls: readonly string[] }) {
  const json = new TextEncoder().encode(JSON.stringify({ code: input.code, urls: input.urls }))
  const base64url = bytesToBase64(json).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "")

  return `${PAGE}#${base64url}`
}

/** The server addresses another device can reach: loopback addresses are useless to the device scanning the code. */
export function reachableURLs(urls: readonly string[]) {
  return urls.filter((value) => {
    const hostname = parse(value)?.hostname

    return (
      hostname !== undefined &&
      hostname !== "localhost" &&
      !hostname.endsWith(".localhost") &&
      !hostname.startsWith("127.") &&
      hostname !== "[::1]"
    )
  })
}

/** The {"code","urls"} JSON a pair page link carries, unvalidated, or undefined for any other text. */
export function pairPagePayload(value: string) {
  const url = parse(value.trim())

  if (!url || url.origin !== new URL(PAGE).origin || (url.pathname !== "/pair" && url.pathname !== "/pair/")) return
  if (url.username || url.password || url.search) return
  const fragment = url.hash.slice(1)

  if (!/^[A-Za-z0-9_-]+={0,2}$/.test(fragment)) return
  const base64 = fragment.replace(/=+$/, "").replaceAll("-", "+").replaceAll("_", "/")

  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(
      base64ToBytes(base64 + "=".repeat((4 - (base64.length % 4)) % 4)),
    )
  } catch {
    return
  }
}

// URL.parse is missing from Safari before 18.
function parse(value: string) {
  try {
    return new URL(value)
  } catch {
    return
  }
}
