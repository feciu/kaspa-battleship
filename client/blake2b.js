// BLAKE2b-256 — needed in the browser because the board has to be hashed exactly
// the same way as in the contract (leaf = blake2b(index ++ value ++ salt),
// node = blake2b(left ++ right)). A 32-bit implementation (no BigInt),
// verified with test vectors against the Rust implementation.
const BLAKE2B_IV32 = new Uint32Array([
  0xf3bcc908, 0x6a09e667, 0x84caa73b, 0xbb67ae85, 0xfe94f82b, 0x3c6ef372, 0x5f1d36f1, 0xa54ff53a,
  0xade682d1, 0x510e527f, 0x2b3e6c1f, 0x9b05688c, 0xfb41bd6b, 0x1f83d9ab, 0x137e2179, 0x5be0cd19,
]);
const SIGMA8 = [
  0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15, 14,10,4,8,9,15,13,6,1,12,0,2,11,7,5,3,
  11,8,12,0,5,2,15,13,10,14,3,6,7,1,9,4, 7,9,3,1,13,12,11,14,2,6,5,10,4,0,15,8,
  9,0,5,7,2,4,10,15,14,1,11,12,6,8,3,13, 2,12,6,10,0,11,8,3,4,13,7,5,15,14,1,9,
  12,5,1,15,14,13,4,10,0,7,6,3,9,2,8,11, 13,11,7,14,12,1,3,9,5,0,15,4,8,6,2,10,
  6,15,14,9,11,3,0,8,12,2,13,7,1,4,10,5, 10,2,8,4,7,6,1,5,15,11,9,14,3,12,13,0,
  0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15, 14,10,4,8,9,15,13,6,1,12,0,2,11,7,5,3,
];
const SIGMA82 = new Uint8Array(SIGMA8.map((x) => x * 2));
const v = new Uint32Array(32), m = new Uint32Array(32);

function ADD64AA(a, i, j) {
  const o0 = a[i] + a[j], o1 = a[i + 1] + a[j + 1] + (o0 >= 0x100000000 ? 1 : 0);
  a[i] = o0; a[i + 1] = o1;
}
function ADD64AC(a, i, b0, b1) {
  let o0 = a[i] + b0; if (b0 < 0) o0 += 0x100000000;
  const o1 = a[i + 1] + b1 + (o0 >= 0x100000000 ? 1 : 0);
  a[i] = o0; a[i + 1] = o1;
}
const B2B_GET32 = (arr, i) => arr[i] ^ (arr[i + 1] << 8) ^ (arr[i + 2] << 16) ^ (arr[i + 3] << 24);

function B2B_G(a, b, c, d, ix, iy) {
  const x0 = m[ix], x1 = m[ix + 1], y0 = m[iy], y1 = m[iy + 1];
  ADD64AA(v, a, b); ADD64AC(v, a, x0, x1);
  let xor0 = v[d] ^ v[a], xor1 = v[d + 1] ^ v[a + 1];
  v[d] = xor1; v[d + 1] = xor0;                       // rotr 32
  ADD64AA(v, c, d);
  xor0 = v[b] ^ v[c]; xor1 = v[b + 1] ^ v[c + 1];
  v[b] = (xor0 >>> 24) ^ (xor1 << 8); v[b + 1] = (xor1 >>> 24) ^ (xor0 << 8);   // rotr 24
  ADD64AA(v, a, b); ADD64AC(v, a, y0, y1);
  xor0 = v[d] ^ v[a]; xor1 = v[d + 1] ^ v[a + 1];
  v[d] = (xor0 >>> 16) ^ (xor1 << 16); v[d + 1] = (xor1 >>> 16) ^ (xor0 << 16); // rotr 16
  ADD64AA(v, c, d);
  xor0 = v[b] ^ v[c]; xor1 = v[b + 1] ^ v[c + 1];
  v[b] = (xor1 >>> 31) ^ (xor0 << 1); v[b + 1] = (xor0 >>> 31) ^ (xor1 << 1);   // rotr 63
}

function compress(ctx, last) {
  for (let i = 0; i < 16; i++) { v[i] = ctx.h[i]; v[i + 16] = BLAKE2B_IV32[i]; }
  v[24] ^= ctx.t; v[25] ^= ctx.t / 0x100000000;
  if (last) { v[28] = ~v[28]; v[29] = ~v[29]; }
  for (let i = 0; i < 32; i++) m[i] = B2B_GET32(ctx.b, 4 * i);
  for (let i = 0; i < 12; i++) {
    const s = SIGMA82.subarray(i * 16, i * 16 + 16);
    B2B_G(0, 8, 16, 24, s[0], s[1]);   B2B_G(2, 10, 18, 26, s[2], s[3]);
    B2B_G(4, 12, 20, 28, s[4], s[5]);  B2B_G(6, 14, 22, 30, s[6], s[7]);
    B2B_G(0, 10, 20, 30, s[8], s[9]);  B2B_G(2, 12, 22, 24, s[10], s[11]);
    B2B_G(4, 14, 16, 26, s[12], s[13]); B2B_G(6, 8, 18, 28, s[14], s[15]);
  }
  for (let i = 0; i < 16; i++) ctx.h[i] ^= v[i] ^ v[i + 16];
}

export function blake2b256(...parts) {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const input = new Uint8Array(total);
  let off = 0; for (const p of parts) { input.set(p, off); off += p.length; }

  const ctx = { b: new Uint8Array(128), h: new Uint32Array(16), t: 0, c: 0 };
  for (let i = 0; i < 16; i++) ctx.h[i] = BLAKE2B_IV32[i];
  ctx.h[0] ^= 0x01010000 ^ 32;                        // params: no key, 32-byte output

  for (let i = 0; i < input.length; i++) {
    if (ctx.c === 128) { ctx.t += ctx.c; compress(ctx, false); ctx.c = 0; }
    ctx.b[ctx.c++] = input[i];
  }
  ctx.t += ctx.c;
  while (ctx.c < 128) ctx.b[ctx.c++] = 0;
  compress(ctx, true);

  const out = new Uint8Array(32);
  for (let i = 0; i < 32; i++) out[i] = (ctx.h[i >> 2] >> (8 * (i & 3))) & 0xff;
  return out;
}

export const hex = (u8) => [...u8].map((b) => b.toString(16).padStart(2, "0")).join("");
export const unhex = (s) => new Uint8Array(s.match(/.{2}/g).map((b) => parseInt(b, 16)));
export const le8 = (n) => { const a = new Uint8Array(8); let x = BigInt(n); for (let i = 0; i < 8; i++) { a[i] = Number(x & 0xffn); x >>= 8n; } return a; };
