export type BrowserClaimSecret = {
  readonly bytes: Uint8Array;
  readonly value: string;
};

// The encoded preimage is sent only to the same-origin claim endpoint. Keeping
// the raw bytes alongside it prevents accidentally hashing the encoded text.
export function createBrowserClaimSecret(): BrowserClaimSecret {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return { bytes, value: base64Url(bytes) };
}

export async function digestBrowserClaimSecret(value: Uint8Array): Promise<string> {
  const raw = new ArrayBuffer(value.byteLength);
  new Uint8Array(raw).set(value);
  return base64Url(new Uint8Array(await crypto.subtle.digest("SHA-256", raw)));
}

function base64Url(value: Uint8Array): string {
  return btoa(String.fromCharCode(...value))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
}
