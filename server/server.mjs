// KasPlay Battleship — table server (TESTNET, separate from KasPlay).
//
// The server is a coordinator, not a referee. It builds transactions and covers
// testnet fees, but it does NOT know the players' boards (it only ever sees a
// Merkle root) and does NOT hold their keys — the player's browser signs every
// move. The server can at most refuse service; it cannot lie about the result,
// because the network verifies it.
import { createRequire } from "module";
const require = createRequire(import.meta.url);
// Paths come from env vars — the defaults describe our own installation.
const LAB_DIR = process.env.BS_LAB || new URL("..", import.meta.url).pathname.replace(/\/$/, "");
globalThis.WebSocket = require(process.env.BS_WS_MODULE
  || "websocket").w3cwebsocket;   // set BS_WS_MODULE if the module lives elsewhere
const kaspa = require(process.env.BS_SDK || `${LAB_DIR}/kaspa-sdk/kaspa-wasm32-sdk/nodejs/kaspa/kaspa.js`);
import http from "node:http";
import { readFileSync, writeFileSync, renameSync, existsSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { extname, join, normalize } from "node:path";
import { randomUUID, randomBytes } from "node:crypto";
import { blake2b256, hex, unhex, le8 } from "./public/blake2b.js";

const LAB = LAB_DIR;
const HERE = `${LAB}/battleship`;
const PORT = Number(process.env.BS_PORT || 3010);
const NETWORK = process.env.BS_NETWORK || "testnet-10";
const TABLE_VALUE = 60_000_000;      // 0.6 TKAS sits in the table — contract v2 has a `rescue` entry,
                                     // so the value returns to the operator wallet after the game
const MAX_MISS = 18;                 // 25 - 7: the most misses a fully legal board can take
const STALE_MS = 604_800_000;        // 7 days — after that a table counts as abandoned
const CREATE_BACKDATE = 1_800_000;   // 30 min: the table-creation marker must sit BELOW the
                                     // `join` lockTime (which is itself backdated by 20 min)
const MOVE_WINDOW = 86_400_000;      // default per-move window (24h)
// Chosen when a table is opened. The contract reads `move_window` FROM STATE, so
// seeding it in the genesis is enough — the script honours it with no changes.
const WINDOWS = { "5m": 300_000, "10m": 600_000, "15m": 900_000, "1h": 3_600_000, "15h": 54_000_000, "24h": 86_400_000 };
const WALLET = JSON.parse(readFileSync(process.env.BS_WALLET || `${LAB}/tn10-operator.json`, "utf8"));
const ZERO32 = "00".repeat(32);

const db = () => JSON.parse(readFileSync(`${HERE}/tables.json`, "utf8"));
const save = (d) => { writeFileSync(`${HERE}/tables.json.tmp`, JSON.stringify(d, null, 2)); renameSync(`${HERE}/tables.json.tmp`, `${HERE}/tables.json`); };
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

// ── bridge to the network ─────────────────────────────────────────────────
let rpc = null;
const conn = async () => { if (!rpc) { rpc = new kaspa.RpcClient({ resolver: new kaspa.Resolver(), networkId: NETWORK }); await rpc.connect(); } return rpc; };
const drop = async () => { try { if (rpc) await rpc.disconnect(); } catch {} rpc = null; };
const DEAD = /WebSocket|not connected|disconnect|closed|timeout/i;

// Public nodes can quietly mute a connection after a while idle (lesson from
// KasTip: "zombie subscription"). EVERY RPC call must be able to get back up.
async function rpcCall(what, fn) {
  for (let att = 1; att <= 4; att++) {
    try { return await fn(await conn()); }
    catch (e) {
      const m = (e.message ?? e).toString();
      if (DEAD.test(m) && att < 4) { log(`RPC (${what}) died — reconnecting [${att}]`); await drop(); await new Promise((r) => setTimeout(r, 800)); continue; }
      throw e;
    }
  }
  throw new Error(`RPC ${what}: no connection to a node`);
}
// keep the connection alive so it does not die between games
setInterval(() => { rpcCall("ping", (r) => r.getServerInfo()).catch(() => {}); }, 20_000);

const budget = (s) => { const m = String(s).match(/(\d+)/); return m ? parseInt(m[1]) : 0; };
const toTx = (j) => new kaspa.Transaction({
  version: j.version, lockTime: BigInt(j.lockTime), subnetworkId: "00".repeat(20),
  gas: BigInt(j.gas), payload: j.payload,
  inputs: j.inputs.map((i) => ({ previousOutpoint: { transactionId: i.txid, index: i.index },
    signatureScript: i.signatureScript, sequence: BigInt(i.sequence), sigOpCount: 0, computeBudget: budget(i.computeCommit) })),
  outputs: j.outputs.map((o) => ({ value: BigInt(o.value), scriptPublicKey: { version: o.spkVersion, script: o.spkScript },
    ...(o.covenant ? { covenant: { authorizingInput: o.covenant.authorizingInput, covenantId: o.covenant.covenantId } } : {}) })),
});

async function submit(txJson) {
  for (let att = 1; att <= 6; att++) {
    try { const r = await rpcCall("submit", (c) => c.submitTransaction({ transaction: toTx(txJson), allowOrphan: false })); return r.transactionId; }
    catch (e) {
      const m = (e.message ?? e).toString();
      if (m.includes("already") || m.includes("fully spent")) return txJson.id;
      if (m.includes("WebSocket") || m.includes("not connected")) { await drop(); await new Promise((r) => setTimeout(r, 2000)); continue; }
      if (att >= 3) { walletDirty = true; throw new Error(m.slice(0, 180)); }
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
  throw new Error("submit failed");
}

// ── operator wallet: one UTXO chain, so moves are serialized ──────────────
let walletTip = null;
let chainLock = Promise.resolve();
// Moves share a single UTXO chain, so they must be serialized. But ONE hung
// operation must not block everything queued behind it — hence the timeout.
const withTimeout = (p, ms, what) => Promise.race([p,
  new Promise((_, rej) => setTimeout(() => rej(new Error(`${what}: timed out after ${ms / 1000}s`)), ms))]);
const serialize = (fn) => {
  const run = () => withTimeout(Promise.resolve().then(fn), 40_000, "on-chain operation");
  chainLock = chainLock.then(run, run);
  return chainLock;
};

let walletDirty = false;
async function walletUtxo() {
  if (walletTip && !walletDirty) return walletTip;
  walletDirty = false;
  const r = await rpcCall("utxo", (c) => c.getUtxosByAddresses({ addresses: [WALLET.address] }));
  if (!r.entries.length) throw new Error("operator wallet is empty");
  const best = r.entries.reduce((a, e) => (Number(e.amount) > Number(a.amount) ? e : a));
  walletTip = { txid: best.outpoint.transactionId, index: best.outpoint.index, value: Number(best.amount) };
  return walletTip;
}

function runBuilder(job) {
  const f = `${HERE}/.job-${randomUUID()}.json`;
  writeFileSync(f, JSON.stringify(job));
  try {
    const out = execFileSync(`${LAB}/argent-template/target/release/bs_tx`, [], {
      cwd: `${LAB}/argent-template`, maxBuffer: 32 * 1024 * 1024,
      env: { ...process.env, JOB: f, NETWORK, WALLET_PRIVKEY: WALLET.privateKeyHex,
             PATH: `${process.env.PATH}${process.env.BS_EXTRA_PATH ? ":" + process.env.BS_EXTRA_PATH : ""}` },
    }).toString();
    return JSON.parse(out.trim().split("\n").pop());
  } catch (e) {
    // The builder runs the transaction through the real script engine, so a
    // failure here usually means the network would reject this move.
    const raw = ((e.stderr ?? "") + (e.message ?? "")).toString();
    if (/script failed|verification failed|InputScript|SigHash|Signature/i.test(raw))
      throw new Error("network rejected the move: the proof does not match the frozen board (or it is not your turn)");
    throw new Error("could not build the transaction: " + raw.slice(0, 140));
  } finally { try { require("fs").unlinkSync(f); } catch {} }
}

// ── server-side game rules (mirror of the contract, for the UI) ───────────
const NO_PENDING = 100, SHIP_CELLS = 7;
const bitOf = (cell) => Math.pow(2, cell);                       // 2^cell, cell 0..24
const shotTaken = (mask, cell) => Math.floor(mask / bitOf(cell)) % 2 === 1;

function nextState(st, mode, p, lockTime) {
  const n = { ...st };
  if (mode === "join") {
    n.b_id = p.playerId; n.root_b = p.root; n.turn = 0; n.pending = NO_PENDING;
    n.hits_a = 0; n.hits_b = 0;
    n.miss_a = 0; n.miss_b = 0; n.shots_a = 0; n.shots_b = 0;
    n.deadline = lockTime + (st.move_window || MOVE_WINDOW); n.phase = 1; n.winner = 0;
  } else if (mode === "answer") {
    // CONFIRMING the opponent's shot — the turn STAYS with the responder
    if (p.answer === 1) {
      if (st.turn === 0) n.hits_b = st.hits_b + 1; else n.hits_a = st.hits_a + 1;
    } else {
      // a miss is credited to the SHOOTER — it is what proves the responder's
      // board holds at least 7 ship squares (miss <= 18)
      if (st.turn === 0) n.miss_b = st.miss_b + 1; else n.miss_a = st.miss_a + 1;
    }
    n.pending = NO_PENDING;
    if (n.hits_a >= SHIP_CELLS) { n.phase = 2; n.winner = 0; }
    if (n.hits_b >= SHIP_CELLS) { n.phase = 2; n.winner = 1; }
    n.deadline = lockTime + (st.move_window || MOVE_WINDOW);
  } else if (mode === "shoot") {
    // bitmask of shots fired — the contract rejects a repeat at the same square
    if (st.turn === 0) n.shots_a = st.shots_a + bitOf(p.cell);
    else               n.shots_b = st.shots_b + bitOf(p.cell);
    n.pending = p.cell;
    n.turn = st.turn === 0 ? 1 : 0;
    n.deadline = lockTime + (st.move_window || MOVE_WINDOW);   // window FROM STATE, as in the contract
  } else if (mode === "timeout") {
    n.phase = 2; n.winner = st.turn === 0 ? 1 : 0;
  }
  return n;
}

// Contract v2 requires a locktime strictly greater than the previous move's
// (for `join`: than the table-creation marker). A system clock can jump back on
// an NTP correction, so we enforce the lower bound explicitly.
function moveLockTime(st, mode) {
  const floor = mode === "join" ? st.deadline
                                : st.deadline - (st.move_window || MOVE_WINDOW);
  return Math.max(Date.now() - 1_200_000, floor + 1);
}

// Build and send a move that needs NO player signature (answer / timeout).
// The answer is keyless because the Merkle proof authenticates itself.
async function unsignedMove(t, mode, params) {
  const w = await walletUtxo();
  const lockTime = moveLockTime(t.state, mode);
  const next = nextState(t.state, mode, params, lockTime);
  const r = runBuilder({ mode, wallet: w, covenantId: t.covenantId, outpoint: t.outpoint,
    state: t.state, nextState: next, params, tableValue: TABLE_VALUE, lockTime, prepare: false, playerSig: "" });
  const txid = await submit(r.tx);
  walletTip = r.walletAfter;
  const d = db(); const tt = d.tables.find((x) => x.id === t.id);
  tt.state = next; tt.outpoint = r.outpoint;
  tt.log.push({ kind: mode, txid, at: Date.now(), by: params.playerId, cell: t.state.pending, answer: params.answer });
  save(d); broadcast(t.id);
  log(mode, "→", txid.slice(0, 12));
  return { txid, state: next };
}

const pending = new Map();   // nonce -> prepared move awaiting the player's signature
const createLog = [];        // { ip, at } — every table burns operator TKAS, so we rate-limit
const botPending = new Set();  // tables that already have a bot move scheduled

// ── player presence ───────────────────────────────────────────────────────
// ONLY the opponent knows the result of a shot, so a player needs to know whether
// they are at the table at all. We count open event streams and last activity.
const presence = new Map();   // playerId -> { conns, last }
// Nicknames are purely cosmetic and live next to the game. They never enter the
// on-chain state (only the blake2b of the key counts there), so impersonating
// someone's nickname gains NOTHING — moves are still signed with a key.
const NICKS = `${HERE}/nicks.json`;
let nicks = {};
try { nicks = JSON.parse(readFileSync(NICKS, "utf8")); } catch {}
const cleanNick = (n) => String(n ?? "").replace(/[<>&"'`\r\n\t]/g, "").trim().slice(0, 16);
let nicksDirty = false;
setInterval(() => { if (!nicksDirty) return; nicksDirty = false;
  try { writeFileSync(NICKS, JSON.stringify(nicks)); } catch {} }, 10_000);
const setNick = (id, n) => { const v = cleanNick(n); if (!id || !v || nicks[id] === v) return; nicks[id] = v; nicksDirty = true; };
const nickOf = (id) => nicks[id] || null;

const seen = (id, nick) => { if (!id) return; setNick(id, nick);
  const p = presence.get(id) ?? { conns: 0, last: 0 }; p.last = Date.now(); presence.set(id, p); };
const isOnline = (id) => { const p = presence.get(id); return !!p && (p.conns > 0 || Date.now() - p.last < 25_000); };

// ── live: Server-Sent Events — every state change goes to open tabs ───────
const sseClients = new Set();
function broadcast(tableId) {
  const msg = `data: ${JSON.stringify({ id: tableId, at: Date.now() })}\n\n`;
  for (const res of sseClients) { try { res.write(msg); } catch { sseClients.delete(res); } }
}
setInterval(() => { for (const res of sseClients) { try { res.write(":hb\n\n"); } catch { sseClients.delete(res); } } }, 25_000);

// ── BOT: an opponent for solo play ────────────────────────────────────────
// The bot is an ordinary player whose key and board happen to live on the server
// (it does not need to be trustless — it is a sparring partner). The human plays
// normally: click, get an answer, no identity switching.
const N = 5, CELLS = 25;
const FLEET = [3, 2, 1, 1];   // 7 squares on a 5x5 board
const around = (i) => { const r = (i / N) | 0, c = i % N, o = [];
  for (let dr = -1; dr <= 1; dr++) for (let dc = -1; dc <= 1; dc++) {
    const nr = r + dr, nc = c + dc; if (nr >= 0 && nr < N && nc >= 0 && nc < N) o.push(nr * N + nc); } return o; };
// Random placement is tight on 5x5 (ships may not touch), so it sometimes fails
// to fit the whole fleet. An incomplete board means the opponent CANNOT win —
// hence we retry until the square count adds up.
function botBoard() {
  for (let attempt = 0; attempt < 500; attempt++) {
    const b = tryBoard();
    if (b.reduce((a, x) => a + x, 0) === SHIP_CELLS) return b;
  }
  throw new Error("could not generate a full fleet");
}
function tryBoard() {
  const c = new Array(CELLS).fill(0);
  for (const len of FLEET) for (let t = 0; t < 900; t++) {
    const h = Math.random() < 0.5, r = (Math.random() * N) | 0, col = (Math.random() * N) | 0;
    const idx = []; let ok = true;
    for (let k = 0; k < len; k++) { const rr = h ? r : r + k, cc = h ? col + k : col;
      if (rr >= N || cc >= N) { ok = false; break; } idx.push(rr * N + cc); }
    if (!ok || idx.some((i) => c[i] || around(i).some((a) => !idx.includes(a) && c[a]))) continue;
    idx.forEach((i) => (c[i] = 1)); break;
  }
  return c;
}
function treeOf(cells, master) {
  const salts = [], leaves = [];
  for (let i = 0; i < 128; i++) { const s = blake2b256(unhex(master), le8(i)); salts.push(s);
    leaves.push(blake2b256(le8(i), le8(i < CELLS ? cells[i] : 0), s)); }
  const nodes = [leaves];
  for (let l = 0; l < 7; l++) { const p = nodes[l], n = [];
    for (let j = 0; j < p.length / 2; j++) n.push(blake2b256(p[2 * j], p[2 * j + 1])); nodes.push(n); }
  return { salts, nodes, root: hex(nodes[7][0]) };
}
const proofOf = (t, i) => [...Array(7)].map((_, l) => hex(t.nodes[l][(i >> l) ^ 1]));

// makes a move signed with a key we hold ourselves (bot only!)
async function signedMoveLocal(t, mode, params, privHex) {
  const w = await walletUtxo();
  const lockTime = moveLockTime(t.state, mode);
  const next = nextState(t.state, mode, params, lockTime);
  const base = { mode, wallet: w, covenantId: t.covenantId, outpoint: t.outpoint,
    state: t.state, nextState: next, params, tableValue: TABLE_VALUE, lockTime };
  const { sighash } = runBuilder({ ...base, prepare: true });
  const sig = kaspa.signScriptHash(sighash, new kaspa.PrivateKey(privHex)).slice(2);
  const r = runBuilder({ ...base, prepare: false, playerSig: sig });
  const txid = await submit(r.tx);
  walletTip = r.walletAfter;
  const d = db(); const tt = d.tables.find((x) => x.id === t.id);
  tt.state = next; tt.outpoint = r.outpoint;
  tt.log.push({ kind: mode, txid, at: Date.now(), by: params.playerId, cell: params.cell, answer: params.answer });
  save(d); broadcast(t.id);
  return { txid, state: next };
}

async function botTurn(tableId) { return serialize(() => botTurnInner(tableId)); }
async function botTurnInner(tableId) {
  let d = db(); let t = d.tables.find((x) => x.id === tableId);
  if (!t || !t.botSecret || t.state.phase !== 1) return;
  const bot = t.botSecret;
  const botIsA = t.state.a_id === bot.id;
  const isBotTurn = (st) => (st.turn === 0 && botIsA) || (st.turn === 1 && !botIsA);
  if (!isBotTurn(t.state)) return;
  const tree = treeOf(bot.cells, bot.master);

  // 1) first CONFIRM the player's shot (keyless — the proof speaks for itself)
  if (t.state.pending !== NO_PENDING) {
    const cellShot = t.state.pending;
    const answer = bot.cells[cellShot] ?? 0;
    try {
      await unsignedMove(t, "answer", { answer, leafSalt: hex(tree.salts[cellShot]),
        proof: proofOf(tree, cellShot), playerId: bot.id });
      log(`bot confirms ${cellShot}: ${answer ? "hit" : "miss"}`);
    } catch (e) { log("bot (confirm):", (e.message ?? e).toString().slice(0, 90)); return; }
    d = db(); t = d.tables.find((x) => x.id === tableId);
    if (!t || t.state.phase !== 1 || !isBotTurn(t.state)) return;   // e.g. the player has just won
  }

  // 2) then FIRE
  const myMask = botIsA ? t.state.shots_a : t.state.shots_b;
  const free = [...Array(CELLS).keys()].filter((i) => !shotTaken(myMask, i));
  if (!free.length) return;
  const cell = free[(Math.random() * free.length) | 0];
  try { await signedMoveLocal(t, "shoot", { cell, pubkey: bot.pubkey, playerId: bot.id }, bot.priv);
    log(`bot fires at ${cell}`); }
  catch (e) { log("bot (shot):", (e.message ?? e).toString().slice(0, 90)); }
}

// ── API ───────────────────────────────────────────────────────────────────
const routes = {
  "GET /api/tables": async () => {
    const d = db();
    return { tables: d.tables.map((t) => ({
      id: t.id, name: t.name, phase: t.state.phase, turn: t.state.turn,
      hitsA: t.state.hits_a, hitsB: t.state.hits_b, winner: t.state.winner,
      playerA: t.state.a_id.slice(0, 10), playerB: t.state.b_id === ZERO32 ? null : t.state.b_id.slice(0, 10),
      creatorOnline: isOnline(t.state.a_id),   // an open table is worth only as much as its host's presence
      nickA: t.botSecret && t.state.a_id === t.botSecret.id ? "BOT" : nickOf(t.state.a_id),
      nickB: t.botSecret && t.state.b_id === t.botSecret.id ? "BOT" : nickOf(t.state.b_id),
      moves: t.log.length, created: t.created, covenantId: t.covenantId, vsBot: !!t.botSecret,
      window: t.state.move_window,
      address: t.address, lastTxid: t.log.length ? t.log[t.log.length - 1].txid : t.genesisTxid,
    })) };
  },

  "GET /api/table": async (q) => {
    seen(q.me, q.nick);
    const t = db().tables.find((x) => x.id === q.id);
    if (!t) throw new Error("no such table");
    // a game against the bot must not stall when its move was lost (network error)
    if (t.botSecret && t.state.phase === 1) {
      const botIsA = t.state.a_id === t.botSecret.id;
      const botTurnNow = (t.state.turn === 0 && botIsA) || (t.state.turn === 1 && !botIsA);
      if (botTurnNow && !botPending.has(t.id)) {
        botPending.add(t.id);
        setTimeout(() => { botTurn(t.id).catch(() => {}).finally(() => botPending.delete(t.id)); }, 500);
      }
    }
    const { botSecret, ...safe } = t;          // the bot's board stays on the server
    // the bot never walks away from the table; a human sometimes does
    const online = { a: !!botSecret && t.state.a_id === botSecret.id ? true : isOnline(t.state.a_id),
                     b: !!botSecret && t.state.b_id === botSecret.id ? true : isOnline(t.state.b_id) };
    const nick = { a: botSecret && t.state.a_id === botSecret.id ? "BOT" : nickOf(t.state.a_id),
                   b: botSecret && t.state.b_id === botSecret.id ? "BOT" : nickOf(t.state.b_id) };
    return { table: { ...safe, vsBot: !!botSecret, online, nick } };
  },

  // Player A opens the table: we fund the genesis, because A has nothing on-chain yet.
  "POST /api/table/create": async (b) => serialize(async () => {
    const now = Date.now();
    while (createLog.length && now - createLog[0].at > 3_600_000) createLog.shift();
    // The limits protect the operator wallet from a runaway loop, not from players.
    // Since v3 `rescue` reclaims the table deposit, the real cost is just fees —
    // hence the loose thresholds. A tight per-IP limit used to cut off whole
    // offices and mobile networks (CGNAT), where many players share one address.
    if (createLog.length >= 120) throw new Error("hourly limit of new tables reached — please try later");
    if (createLog.filter((c) => c.ip === b._ip).length >= 25) throw new Error("too many tables from this address — try again in an hour");
    createLog.push({ ip: b._ip, at: now });
    const w = await walletUtxo();
    const st = { a_id: b.playerId, b_id: ZERO32, root_a: b.root, root_b: ZERO32,
      turn: 0, pending: NO_PENDING, hits_a: 0, hits_b: 0,
      miss_a: 0, miss_b: 0, shots_a: 0, shots_b: 0,
      // v2: in the WAITING phase `deadline` is the TABLE-CREATION MARKER — the lower
      // bound for the `join` locktime. Backdated by 30 min, because every move gets
      // a lockTime 20 min in the past (median block time lags the wall clock).
      deadline: Date.now() - CREATE_BACKDATE,
      move_window: WINDOWS[b.window] ?? MOVE_WINDOW, phase: 0, winner: 0 };
    const r = runBuilder({ mode: "create", wallet: w, state: st, tableValue: TABLE_VALUE });
    const txid = await submit(r.tx);
    walletTip = r.walletAfter;
    const addr = kaspa.addressFromScriptPublicKey(new kaspa.ScriptPublicKey(0, r.tx.outputs[0].spkScript), NETWORK).toString();
    const d = db();
    d.tables.push({ id: randomUUID().slice(0, 8), name: b.name || "Table", covenantId: r.covenantId,
      outpoint: r.outpoint, state: st, address: addr, genesisTxid: txid, created: Date.now(),
      log: [{ kind: "create", txid, at: Date.now(), by: "A" }] });
    save(d);
    broadcast(d.tables[d.tables.length - 1].id);
    const created = d.tables[d.tables.length - 1];
    log("table created", txid.slice(0, 12));
    if (b.vsBot) {
      // the bot gets its own key and board, and sits down at once
      const priv = hex(randomBytes(32));
      const pk = new kaspa.PrivateKey(priv);
      const pubkey = pk.toPublicKey().toXOnlyPublicKey().toString();
      const cells = botBoard(), master = hex(randomBytes(32));
      created.botSecret = { priv, pubkey, id: hex(blake2b256(unhex(pubkey))), cells, master };
      save(d);
      const t = db().tables.find((x) => x.id === created.id);
      t.botSecret = created.botSecret;
      try {
        await signedMoveLocal(t, "join", { root: treeOf(cells, master).root, pubkey, playerId: created.botSecret.id }, priv);
        log("bot sat down at table", created.id);
      } catch (e) { log("bot failed to sit down:", (e.message ?? e).toString().slice(0, 90)); }
    }
    return { ok: true, txid, id: created.id, address: addr, vsBot: !!b.vsBot };
  }),

  // A move (join/play/timeout) in two steps: digest to sign, then the signature.
  "POST /api/move/prepare": async (b) => serialize(async () => {
    const d = db();
    const t = d.tables.find((x) => x.id === b.id);
    if (!t) throw new Error("no such table");
    // The contract enforces the same rules — we check them here only so the player
    // gets a readable message instead of a raw rejection from the network.
    if (b.mode === "shoot") {
      if (t.state.pending !== NO_PENDING)
        throw new Error("their shot must be confirmed first");
      const mask = t.state.turn === 0 ? t.state.shots_a : t.state.shots_b;
      if (shotTaken(mask, b.params.cell))
        throw new Error("that square has already been fired at — pick another");
    }
    const w = await walletUtxo();   // current tip; commit verifies it has not changed
    const lockTime = moveLockTime(t.state, b.mode);
    const next = nextState(t.state, b.mode, b.params, lockTime);
    const job = { mode: b.mode, wallet: w, covenantId: t.covenantId, outpoint: t.outpoint,
      state: t.state, nextState: next, params: b.params, tableValue: TABLE_VALUE, lockTime, prepare: true };
    const r = runBuilder(job);
    const nonce = randomUUID();
    pending.set(nonce, { job: { ...job, prepare: false }, tableId: t.id, next, mode: b.mode });
    setTimeout(() => pending.delete(nonce), 300_000);
    return { nonce, sighash: r.sighash };
  }),

  "POST /api/move/commit": async (b) => serialize(async () => {
    const p = pending.get(b.nonce);
    if (!p) throw new Error("STALE");
    pending.delete(b.nonce);
    // The player signed the digest of a transaction built on ONE SPECIFIC wallet UTXO.
    // If another move spent that UTXO meanwhile, the signature is already void — we
    // signal STALE and the browser simply rebuilds and signs again.
    const w = await walletUtxo();
    if (p.job.wallet.txid !== w.txid || p.job.wallet.index !== w.index) throw new Error("STALE");
    // The same applies to the TABLE: `prepare` snapshots the game state and the
    // covenant outpoint, and the opponent's answer can land in between — which
    // moves the covenant on and voids the snapshot. Since v3 splits a turn into
    // two transactions, the table advances twice per turn, so this race is easy
    // to hit in a live human game. Detect it here instead of letting the node
    // reject the move with an error that reads like the player cheated.
    const cur = db().tables.find((x) => x.id === p.tableId);
    if (!cur || cur.outpoint?.txid !== p.job.outpoint.txid || cur.outpoint?.index !== p.job.outpoint.index)
      throw new Error("STALE");
    const r = runBuilder({ ...p.job, playerSig: b.sig });
    const txid = await submit(r.tx);
    walletTip = r.walletAfter;
    const d = db();
    const t = d.tables.find((x) => x.id === p.tableId);
    t.state = p.next; t.outpoint = r.outpoint;
    t.log.push({ kind: p.mode, txid, at: Date.now(), by: p.job.params?.playerId,
      cell: p.job.params?.cell, answer: p.job.params?.answer });
    save(d);
    broadcast(p.tableId);
    log(p.mode, "→", txid.slice(0, 12));
    if (t.botSecret && !botPending.has(p.tableId)) {
      botPending.add(p.tableId);
      setTimeout(() => { botTurn(p.tableId).catch(() => {}).finally(() => botPending.delete(p.tableId)); }, 700);
    }
    return { ok: true, txid, state: t.state };
  }),
};

// anyone can trigger a timeout — no player signature needed
routes["POST /api/move/timeout"] = async (b) => serialize(async () => {
  const d = db();
  const t = d.tables.find((x) => x.id === b.id);
  if (!t) throw new Error("no such table");
  if (t.state.phase !== 1) throw new Error("the game is not running");
  // The contract compares the locktime (ours: 20 min back, because median block time
  // lags the wall clock) with the deadline — so the network only accepts a timeout
  // ~20 min after it expires. Without this gate the script error would be confusing.
  if (Date.now() - 1_200_000 < t.state.deadline) {
    if (Date.now() < t.state.deadline) throw new Error("the move deadline has not passed yet");
    const min = Math.ceil((t.state.deadline + 1_200_000 - Date.now()) / 60_000);
    throw new Error(`the deadline passed, but the network will only accept it in ~${min} min (median block time)`);
  }
  const w = await walletUtxo();
  const lockTime = moveLockTime(t.state, "timeout");
  const next = nextState(t.state, "timeout", {}, lockTime);
  const r = runBuilder({ mode: "timeout", wallet: w, covenantId: t.covenantId, outpoint: t.outpoint,
    state: t.state, nextState: next, params: {}, tableValue: TABLE_VALUE, lockTime, prepare: false, playerSig: "" });
  const txid = await submit(r.tx);
  walletTip = r.walletAfter;
  t.state = next; t.outpoint = r.outpoint;
  t.log.push({ kind: "timeout", txid, at: Date.now() });
  save(d);
  broadcast(t.id);
  return { ok: true, txid, state: next };
});

// CONFIRMING the opponent's shot — a single request with no signing round, because
// the `answer` entry is keyless. The DEFENDER's browser sends it automatically.
routes["POST /api/move/answer"] = async (b) => serialize(async () => {
  const d = db();
  const t = d.tables.find((x) => x.id === b.id);
  if (!t) throw new Error("no such table");
  if (t.state.phase !== 1) throw new Error("the game is not running");
  if (t.state.pending === NO_PENDING) throw new Error("there is nothing to confirm");
  const myMiss = t.state.turn === 0 ? t.state.miss_b : t.state.miss_a;
  if (b.params.answer === 0 && myMiss + 1 > MAX_MISS)
    throw new Error("the board does not hold a full fleet — the network would reject this confirmation");
  const r = await unsignedMove(t, "answer", b.params);
  const tt = db().tables.find((x) => x.id === b.id);
  if (tt?.botSecret && !botPending.has(b.id)) {
    botPending.add(b.id);
    setTimeout(() => { botTurn(b.id).catch(() => {}).finally(() => botPending.delete(b.id)); }, 500);
  }
  return { ok: true, txid: r.txid, state: r.state };
});

// DEADLINE WATCHDOG. A player waiting on an abandoned game should not have to click
// anything — once the deadline passes (plus room for median block time) we claim
// the timeout ourselves and the game ends in their favour.
// It also closes the race described in the audit: first to claim wins.
setInterval(async () => {
  let tables;
  try { tables = db().tables; } catch { return; }
  const netNow = Date.now() - 1_200_000;   // same scale as lockTime and deadline
  // 1) abandoned games — settle them once the deadline passes
  for (const t of tables.filter((t) => t.state.phase === 1 && t.state.deadline > 0 && netNow >= t.state.deadline + 30_000)) {
    try {
      const r = await routes["POST /api/move/timeout"]({ id: t.id });
      log(`watchtower: ${t.id} settled on timeout → ${r.txid.slice(0, 12)}`);
    } catch (e) { log(`watchtower ${t.id}:`, (e.message ?? e).toString().slice(0, 80)); }
  }
  // 2) open tables nobody ever sat down at — the contract lets us close them
  //    STALE_MS after creation (in the WAITING phase `deadline` is the creation
  //    marker), so they do not stay on-chain forever
  for (const t of tables.filter((t) => t.state.phase === 0 && !t.rescued && netNow >= t.state.deadline + STALE_MS)) {
    try {
      const r = await routes["POST /api/table/rescue"]({ id: t.id });
      log(`watchtower: open table ${t.id} expired and was closed → ${r.txid.slice(0, 12)}`);
    } catch (e) { log(`watchtower (expired) ${t.id}:`, (e.message ?? e).toString().slice(0, 80)); }
  }
  // 3) finished tables — close the covenant and reclaim its value
  for (const t of tables.filter((t) => t.state.phase === 2 && !t.rescued && Date.now() - (t.log[t.log.length - 1]?.at ?? 0) > 60_000)) {
    try {
      const r = await routes["POST /api/table/rescue"]({ id: t.id });
      log(`watchtower: ${t.id} closed, deposit reclaimed → ${r.txid.slice(0, 12)}`);
    } catch (e) { log(`watchtower (close) ${t.id}:`, (e.message ?? e).toString().slice(0, 80)); }
  }
}, 60_000);

// Reclaiming the table value (v2). Keyless like timeout — the covenant disappears
// and 0.6 TKAS returns to the operator wallet instead of burning forever.
routes["POST /api/table/rescue"] = async (b) => serialize(async () => {
  const d = db();
  const t = d.tables.find((x) => x.id === b.id);
  if (!t) throw new Error("no such table");
  if (t.rescued) throw new Error("this table has already been settled");
  const lockTime = Date.now() - 1_200_000;
  const stale = lockTime >= t.state.deadline + STALE_MS;
  if (t.state.phase !== 2 && !stale) throw new Error("the table is still alive");
  const w = await walletUtxo();
  const r = runBuilder({ mode: "rescue", wallet: w, covenantId: t.covenantId, outpoint: t.outpoint,
    state: t.state, params: {}, tableValue: TABLE_VALUE, lockTime, prepare: false, playerSig: "" });
  const txid = await submit(r.tx);
  walletTip = r.walletAfter;
  t.rescued = txid; t.outpoint = null;
  t.log.push({ kind: "rescue", txid, at: Date.now() });
  save(d); broadcast(t.id);
  log("rescue →", txid.slice(0, 12));
  return { ok: true, txid };
});

// ── HTTP ──────────────────────────────────────────────────────────────────
const MIME = { ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".css": "text/css",
  ".wasm": "application/wasm", ".json": "application/json", ".svg": "image/svg+xml",
  ".png": "image/png", ".jpg": "image/jpeg", ".webp": "image/webp", ".ico": "image/x-icon" };

http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://x");
  const key = `${req.method} ${url.pathname}`;
  const handler = routes[key];
  if (handler) {
    let body = {};
    if (req.method === "POST") {
      const chunks = []; for await (const c of req) chunks.push(c);
      body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {};
      body._ip = req.headers["x-real-ip"] || req.socket.remoteAddress;
      if (body.playerId && body.nick) setNick(body.playerId, body.nick);
      if (body.params?.playerId && body.nick) setNick(body.params.playerId, body.nick);
    }
    try {
      const out = await handler(req.method === "POST" ? body : Object.fromEntries(url.searchParams));
      res.writeHead(200, { "content-type": "application/json" }); res.end(JSON.stringify(out));
    } catch (e) {
      log("ERROR", key, (e.message ?? e).toString().slice(0, 160));
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: (e.message ?? e).toString().slice(0, 300) }));
    }
    return;
  }
  if (req.method === "GET" && url.pathname === "/api/events") {
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache",
      "x-accel-buffering": "no", connection: "keep-alive" });
    res.write(":ok\n\n");
    sseClients.add(res);
    const me = url.searchParams.get("me");
    if (me) { setNick(me, url.searchParams.get("nick"));
      const p = presence.get(me) ?? { conns: 0, last: 0 }; p.conns++; p.last = Date.now(); presence.set(me, p); }
    req.on("close", () => {
      sseClients.delete(res);
      if (me) { const p = presence.get(me); if (p) { p.conns = Math.max(0, p.conns - 1); p.last = Date.now(); } }
    });
    return;
  }
  let p = normalize(url.pathname === "/" ? "/index.html" : url.pathname).replace(/^(\.\.[/\\])+/, "");
  const file = join(`${HERE}/public`, p);
  if (!file.startsWith(`${HERE}/public`) || !existsSync(file)) { res.writeHead(404); res.end("not found"); return; }
  res.writeHead(200, { "content-type": MIME[extname(file)] ?? "application/octet-stream", "cache-control": "no-store" });
  res.end(readFileSync(file));
}).listen(PORT, "127.0.0.1", async () => {
  log(`Battleship: http://127.0.0.1:${PORT} (network ${NETWORK})`);
  // warm-up: connect to a node and fetch the wallet UTXO in the background so the
  // FIRST player does not pay a dozen seconds for a cold start
  try { const t = Date.now(); await walletUtxo();
    log(`wallet ready in ${((Date.now() - t) / 1000).toFixed(1)}s: ${(walletTip.value / 1e8).toFixed(2)} TKAS`); }
  catch (e) { log("warm-up failed:", (e.message ?? e).toString().slice(0, 80)); }
});
