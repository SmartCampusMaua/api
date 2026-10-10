// Shared-secret gate for /api/registry/v0.1/... writes, ahead of any real per-user auth
// (better-auth isn't implemented yet). Deliberately simple: one bearer token, hashed at rest —
// never the plaintext — compared in constant time. Read routes stay open, same posture as the
// rest of this API; only mutations go through this.
import { createHash, timingSafeEqual } from "crypto";

const WRITE_TOKEN_HASH = process.env.REGISTRY_WRITE_TOKEN_HASH;

function sha256Hex(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

// Fails closed: an unconfigured REGISTRY_WRITE_TOKEN_HASH means every write is rejected, not
// silently allowed — a missing env var should never be the thing standing between "anyone can
// register devices" and "no one can."
export function verifyWriteToken(authorizationHeader: string | undefined): boolean {
  if (!WRITE_TOKEN_HASH) return false;
  if (!authorizationHeader?.startsWith("Bearer ")) return false;
  const provided = authorizationHeader.slice("Bearer ".length).trim();
  if (!provided) return false;

  const providedHash = Buffer.from(sha256Hex(provided), "hex");
  const expectedHash = Buffer.from(WRITE_TOKEN_HASH, "hex");
  // timingSafeEqual throws on mismatched lengths rather than returning false — a malformed
  // configured hash shouldn't crash the request, it should just fail verification.
  if (providedHash.length !== expectedHash.length) return false;
  return timingSafeEqual(providedHash, expectedHash);
}
