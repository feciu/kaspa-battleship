# Battleship on Kaspa covenants

A two-player Battleship game where the rules are enforced by Kaspa consensus, not by a server.

The game state **is** a UTXO. Every move **is** a transaction. Lying about a hit doesn't get caught by
an honest operator — it gets the transaction rejected by nodes.

▶ **Play it:** https://kasplay.fun/battleship/ — testnet-10, no wallet needed, fees are covered.
📜 **A real 63-transaction game:** https://kasplay.fun/battleship/#714fa319

---

## The problem this solves

Battleship is interesting on-chain precisely because it needs **hidden information**. Both boards must stay
secret, yet every "hit or miss" must be trustworthy. A normal game server solves this by being trusted.
Here nobody is.

**Commit.** When you place your fleet, your browser builds a 128-leaf Merkle tree and publishes only the
32-byte root. Leaf `i` is:

```
leaf_i = blake2b( le8(i) ‖ le8(value_i) ‖ salt_i )      salt_i = blake2b( master ‖ le8(i) )
```

Per-leaf salts matter: revealing one square tells you nothing about the others, because `salt_i` is not
recoverable from `master` in reverse. The index lives *inside* the leaf, so a proof for one square cannot be
replayed for another. Leaf preimages are 48 bytes and node preimages 64, so there is no leaf/node confusion.

**Reveal.** To answer a shot you publish the square's value, its salt and seven sibling hashes. The contract
recomputes the path — the branch direction at level `l` comes from bit `l` of the index, which it reads from
its own state, not from you — and requires the result to equal the root you froze before the game started.
Flip one bit of the answer and you land on a different hash. Transaction rejected.

## What consensus actually enforces

1. **Truthful answers** — Merkle proof against your own frozen root.
2. **Turn order** — a shot must be signed by the key whose turn it is.
3. **No firing twice at the same square** — a 25-bit shot mask lives in the state.
4. **A full fleet** — see below; no ZK required.
5. **A monotonic clock** — no rewinding a deadline to time your opponent out early.

## What it does not enforce

- Your browser signs a transaction **built by the server**, without reconstructing and checking it. A
  malicious operator could swap your move. Closing this means rebuilding the transaction client-side.
- Consensus checks the fleet's **size**, not its **shape**: seven single-square ships pass like a legal layout.
- Hence: testnet only, nothing at stake.

---

## Design notes

### The answer needs no signature

This is the decision that made the game playable. A Merkle proof authenticates itself — you cannot produce a
path to a false value — so `answer` is a **keyless entry**. Anyone holding the proof may publish it, and only
the truth is publishable.

The practical consequence: your opponent's browser confirms your shot **automatically**, with no decision from
them. You learn whether you hit in about a second, and only then do they take their time choosing their own
target. The obvious design carries the answer and the next shot in a single entry — but then you have to wait for your
opponent to *think* before you learn the result of your own move. Splitting it into a keyless `answer` and a
signed `shoot` is what turns this from correspondence chess into a game.

### `2^cell` with no loops and no variable exponents

Shot dedup needs bit `cell` of a 25-bit mask. Argent has no loops, and exponentiation with a variable exponent
doesn't exist. Instead of unrolling 25 branches, decompose the exponent into bits:

```
2^cell = 2^b0 · 4^b1 · 16^b2 · 256^b3 · 65536^b4
```

Five `if`s instead of twenty-five, identical result for `cell ∈ 0..24`.

### Proving a full fleet without ZK

Freeze an empty board and you are unsinkable — unless misses are counted. With dedup enforced,
`hits + misses = 25` per board, so a legal 7-cell fleet leaves exactly 18 empty squares:

```
require(new_miss_a <= 18);   // 25 - 7
require(new_miss_b <= 18);
```

The elegance is in who it hits: the transaction carrying the nineteenth miss is built by the **answerer** —
the owner of the illegal board. They cannot move, so they run out of time and lose. An honest player is never
blocked, because reaching 19 distinct misses on a legal board is impossible.

### Nothing is left hanging

`timeout` is keyless, so any relayer can settle an abandoned game; the player whose turn it was loses. A
`rescue` entry then closes the covenant so the table deposit returns to circulation instead of burning. It is
easy to forget this one: every other entry requires `next.value == self.value`, so without an explicit exit a
covenant quietly destroys its deposit forever. `rescue` also covers tables nobody ever joined, after a stale
period.

### Clock gotcha

`require(tx.locktime > deadline - move_window)` gives a monotonic game clock, since `deadline - move_window`
is exactly the previous move's locktime. Two things to know:

- Below `LOCK_TIME_THRESHOLD` (`500_000_000_000`) a node reads locktime as a **DAA score**, not a timestamp.
- Nodes compare locktime against the **median block time**, which lags the wall clock. Every move here is
  stamped ~20 minutes in the past. That offset cancels out in the timeout comparison (both sides are shifted
  equally), so a 5-minute window really means five minutes — but a client comparing `deadline` to `Date.now()`
  will think the deadline passed 20 minutes early. Compare in the same frame.

---

## Layout

```
contracts/battleship.ag      the covenant — five entries: join, answer, shoot, timeout, rescue
builder/bs_tx.rs             transaction builder: `prepare` returns a sighash, `finalize` takes the signature
server/server.mjs            table server: builds transactions, pays fees, knows no boards and holds no keys
client/index.html            the whole client — placement, boards, live network console
client/blake2b.js            32-bit BLAKE2b-256, verified byte-identical against the Rust implementation
```

The contract is written in [Argent](https://github.com/argent-lang/argent), an actor language over
SilverScript, and uses KIP-20 covenants. It compiles to ~1.8 KB of script and 1400 opcodes.

## Running it

Requires a built `argentc`, the Kaspa WASM SDK, and a funded testnet wallet. Paths come from the environment:

```
BS_LAB=/path/to/lab            # argent-template lives here
BS_WALLET=/path/to/wallet.json # { "address": "kaspatest:...", "privateKeyHex": "..." }
BS_SDK=/path/to/kaspa.js
BS_WS_MODULE=/path/to/websocket
BS_PORT=3010
BS_NETWORK=testnet-10
node server/server.mjs
```

The server pays all fees, so a player needs no wallet. It never sees a board — only Merkle roots — and never
holds a player key: the browser signs the sighash itself. The worst a hostile operator can do is refuse
service or swap an unverified move (see above); it cannot fake a result, because the network checks the proof.

## Status

Testnet demo, actively developed. Contracts are unaudited and SilverScript itself is experimental — do not
put real money behind this.

## License

MIT.
