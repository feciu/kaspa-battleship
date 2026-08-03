// KasPlay Statki — serwer stolików (TESTNET, poza KasPlay).
//
// Rola serwera: koordynator, nie sędzia. Buduje transakcje i pokrywa opłaty
// testnetowe, ale NIE zna plansz graczy (dostaje tylko korzeń Merkle'a) i NIE
// ma ich kluczy — ruch podpisuje przeglądarka gracza. Serwer może najwyżej
// odmówić obsługi; nie może skłamać o wyniku, bo to weryfikuje sieć.
import { createRequire } from "module";
const require = createRequire("file:///root/argent-lab/");
// Ścieżki przez zmienne środowiskowe — domyślne wartości to nasza instalacja.
const LAB_DIR = process.env.BS_LAB || "/root/argent-lab";
globalThis.WebSocket = require(process.env.BS_WS_MODULE
  || "websocket").w3cwebsocket;   // ustaw BS_WS_MODULE, jeśli moduł leży gdzie indziej
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
const TABLE_VALUE = 60_000_000;      // 0.6 TKAS w stoliku — kontrakt v2 ma entry `rescue`,
                                     // więc po partii wartość wraca do portfela operatora
const MAX_MISS = 18;                 // 25 - 7: tyle pudeł mieści najlegalniejsza plansza
const STALE_MS = 604_800_000;        // 7 dni — po tylu stół uznajemy za porzucony
const CREATE_BACKDATE = 1_800_000;   // 30 min: znacznik założenia stołu musi leżeć PONIŻEJ
                                     // lockTime'u joinu (ten jest cofnięty o 20 min)
const MOVE_WINDOW = 86_400_000;      // domyślne okno na ruch (24h)
// Do wyboru przy zakładaniu stołu. Kontrakt czyta `move_window` ZE STANU, więc
// wystarczy je zasiać w genesis — skrypt honoruje je bez żadnej zmiany.
const WINDOWS = { "5m": 300_000, "10m": 600_000, "15m": 900_000, "1h": 3_600_000, "15h": 54_000_000, "24h": 86_400_000 };
const WALLET = JSON.parse(readFileSync(process.env.BS_WALLET || `${LAB}/tn10-operator.json`, "utf8"));
const ZERO32 = "00".repeat(32);

const db = () => JSON.parse(readFileSync(`${HERE}/tables.json`, "utf8"));
const save = (d) => { writeFileSync(`${HERE}/tables.json.tmp`, JSON.stringify(d, null, 2)); renameSync(`${HERE}/tables.json.tmp`, `${HERE}/tables.json`); };
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

// ── most do sieci ─────────────────────────────────────────────────────────
let rpc = null;
const conn = async () => { if (!rpc) { rpc = new kaspa.RpcClient({ resolver: new kaspa.Resolver(), networkId: NETWORK }); await rpc.connect(); } return rpc; };
const drop = async () => { try { if (rpc) await rpc.disconnect(); } catch {} rpc = null; };
const DEAD = /WebSocket|not connected|disconnect|closed|timeout/i;

// Publiczne węzły potrafią uciszyć połączenie po chwili bezczynności (lekcja
// z KasTip: "zombie subscription"). KAŻDE wywołanie RPC musi umieć wstać.
async function rpcCall(what, fn) {
  for (let att = 1; att <= 4; att++) {
    try { return await fn(await conn()); }
    catch (e) {
      const m = (e.message ?? e).toString();
      if (DEAD.test(m) && att < 4) { log(`RPC (${what}) padło — łączę ponownie [${att}]`); await drop(); await new Promise((r) => setTimeout(r, 800)); continue; }
      throw e;
    }
  }
  throw new Error(`RPC ${what}: no connection to a node`);
}
// utrzymuj połączenie żywe, żeby nie umierało między partiami
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

// ── portfel operatora: jeden łańcuch UTXO, więc ruchy serializujemy ───────
let walletTip = null;
let chainLock = Promise.resolve();
// Ruchy idą jednym łańcuchem UTXO, więc muszą być szeregowane. Ale JEDNA
// zawieszona operacja nie może zablokować wszystkich następnych — stąd limit.
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
             PATH: `${process.env.PATH}:/root/.cargo/bin` },
    }).toString();
    return JSON.parse(out.trim().split("\n").pop());
  } catch (e) {
    // Builder wykonuje transakcję w prawdziwym silniku skryptowym, więc jego
    // porażka zwykle znaczy: sieć odrzuciłaby ten ruch.
    const raw = ((e.stderr ?? "") + (e.message ?? "")).toString();
    if (/script failed|verification failed|InputScript|SigHash|Signature/i.test(raw))
      throw new Error("network rejected the move: the proof does not match the frozen board (or it is not your turn)");
    throw new Error("could not build the transaction: " + raw.slice(0, 140));
  } finally { try { require("fs").unlinkSync(f); } catch {} }
}

// ── reguły gry po stronie serwera (lustro kontraktu, do podglądu w UI) ────
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
    // POTWIERDZENIE strzału rywala — tura ZOSTAJE przy odpowiadającym
    if (p.answer === 1) {
      if (st.turn === 0) n.hits_b = st.hits_b + 1; else n.hits_a = st.hits_a + 1;
    } else {
      // pudło zapisuje się STRZELAJĄCEMU — to ono dowodzi, że plansza
      // odpowiadającego ma co najmniej 7 pól statków (miss <= 18)
      if (st.turn === 0) n.miss_b = st.miss_b + 1; else n.miss_a = st.miss_a + 1;
    }
    n.pending = NO_PENDING;
    if (n.hits_a >= SHIP_CELLS) { n.phase = 2; n.winner = 0; }
    if (n.hits_b >= SHIP_CELLS) { n.phase = 2; n.winner = 1; }
    n.deadline = lockTime + (st.move_window || MOVE_WINDOW);
  } else if (mode === "shoot") {
    // bitmaska oddanych strzałów — kontrakt odrzuci powtórkę w to samo pole
    if (st.turn === 0) n.shots_a = st.shots_a + bitOf(p.cell);
    else               n.shots_b = st.shots_b + bitOf(p.cell);
    n.pending = p.cell;
    n.turn = st.turn === 0 ? 1 : 0;
    n.deadline = lockTime + (st.move_window || MOVE_WINDOW);   // okno ZE STANU, jak w kontrakcie
  } else if (mode === "timeout") {
    n.phase = 2; n.winner = st.turn === 0 ? 1 : 0;
  }
  return n;
}

// Kontrakt v2 wymaga locktime ostro większego niż locktime poprzedniego ruchu
// (dla `join`: niż znacznik założenia stołu). Zegar systemowy potrafi się cofnąć
// przy korekcie NTP, więc dolną granicę wymuszamy jawnie.
function moveLockTime(st, mode) {
  const floor = mode === "join" ? st.deadline
                                : st.deadline - (st.move_window || MOVE_WINDOW);
  return Math.max(Date.now() - 1_200_000, floor + 1);
}

// Zbuduj i wyślij ruch, który NIE wymaga podpisu gracza (answer / timeout).
// Odpowiedź jest keyless, bo dowód Merkle'a uwierzytelnia sam siebie.
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

const pending = new Map();   // nonce -> przygotowany ruch czekający na podpis gracza
const createLog = [];        // { ip, at } — każdy stół pali TKAS operatora, więc limitujemy tempo
const botPending = new Set();  // stoły, dla których ruch bota już zaplanowany

// ── obecność graczy ───────────────────────────────────────────────────────
// Wynik strzału zna WYŁĄCZNIE rywal, więc gracz musi wiedzieć, czy rywal w ogóle
// siedzi przy stole. Liczymy otwarte strumienie zdarzeń i ostatnią aktywność.
const presence = new Map();   // playerId -> { conns, last }
// Nicki: czysto kosmetyczne, trzymane obok gry. Nie wchodzą do stanu na łańcuchu
// (tam liczy się wyłącznie blake2b klucza), więc podszycie się pod cudzy nick
// nie daje NIC — ruchy i tak podpisuje klucz.
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

// ── live: Server-Sent Events — każda zmiana stanu leci do otwartych kart ──
const sseClients = new Set();
function broadcast(tableId) {
  const msg = `data: ${JSON.stringify({ id: tableId, at: Date.now() })}\n\n`;
  for (const res of sseClients) { try { res.write(msg); } catch { sseClients.delete(res); } }
}
setInterval(() => { for (const res of sseClients) { try { res.write(":hb\n\n"); } catch { sseClients.delete(res); } } }, 25_000);

// ── BOT: przeciwnik do gry solo ───────────────────────────────────────────
// Bot to zwykły gracz, tylko jego klucz i plansza leżą na serwerze (nie musi
// być trustless — to sparingpartner). Gracz gra normalnie: klika i po chwili
// dostaje odpowiedź, bez przełączania tożsamości.
const N = 5, CELLS = 25;
const FLEET = [3, 2, 1, 1];   // 7 pól na planszy 5x5
const around = (i) => { const r = (i / N) | 0, c = i % N, o = [];
  for (let dr = -1; dr <= 1; dr++) for (let dc = -1; dc <= 1; dc++) {
    const nr = r + dr, nc = c + dc; if (nr >= 0 && nr < N && nc >= 0 && nc < N) o.push(nr * N + nc); } return o; };
// Losowanie bywa ciasne na 5x5 (statki nie mogą się stykać), więc czasem nie
// mieści całej floty. Niepełna plansza = przeciwnik NIE MOŻE wygrać — dlatego
// losujemy do skutku i sprawdzamy sumę pól.
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

// wykonuje ruch podpisany kluczem trzymanym po naszej stronie (tylko bot!)
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

// bot odpowiada na strzał gracza i sam strzela
async function botTurn(tableId) { return serialize(() => botTurnInner(tableId)); }
async function botTurnInner(tableId) {
  let d = db(); let t = d.tables.find((x) => x.id === tableId);
  if (!t || !t.botSecret || t.state.phase !== 1) return;
  const bot = t.botSecret;
  const botIsA = t.state.a_id === bot.id;
  const isBotTurn = (st) => (st.turn === 0 && botIsA) || (st.turn === 1 && !botIsA);
  if (!isBotTurn(t.state)) return;
  const tree = treeOf(bot.cells, bot.master);

  // 1) najpierw POTWIERDŹ strzał gracza (bez podpisu — dowód mówi sam za siebie)
  if (t.state.pending !== NO_PENDING) {
    const cellShot = t.state.pending;
    const answer = bot.cells[cellShot] ?? 0;
    try {
      await unsignedMove(t, "answer", { answer, leafSalt: hex(tree.salts[cellShot]),
        proof: proofOf(tree, cellShot), playerId: bot.id });
      log(`bot potwierdza ${cellShot}: ${answer ? "trafiony" : "pudło"}`);
    } catch (e) { log("bot (potwierdzenie):", (e.message ?? e).toString().slice(0, 90)); return; }
    d = db(); t = d.tables.find((x) => x.id === tableId);
    if (!t || t.state.phase !== 1 || !isBotTurn(t.state)) return;   // np. gracz właśnie wygrał
  }

  // 2) potem STRZEL
  const myMask = botIsA ? t.state.shots_a : t.state.shots_b;
  const free = [...Array(CELLS).keys()].filter((i) => !shotTaken(myMask, i));
  if (!free.length) return;
  const cell = free[(Math.random() * free.length) | 0];
  try { await signedMoveLocal(t, "shoot", { cell, pubkey: bot.pubkey, playerId: bot.id }, bot.priv);
    log(`bot strzela ${cell}`); }
  catch (e) { log("bot (strzał):", (e.message ?? e).toString().slice(0, 90)); }
}

// ── API ───────────────────────────────────────────────────────────────────
const routes = {
  "GET /api/tables": async () => {
    const d = db();
    return { tables: d.tables.map((t) => ({
      id: t.id, name: t.name, phase: t.state.phase, turn: t.state.turn,
      hitsA: t.state.hits_a, hitsB: t.state.hits_b, winner: t.state.winner,
      playerA: t.state.a_id.slice(0, 10), playerB: t.state.b_id === ZERO32 ? null : t.state.b_id.slice(0, 10),
      creatorOnline: isOnline(t.state.a_id),   // wolny stół jest wart tyle, ile obecność zakładającego
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
    // partia z botem nie może stać w miejscu, gdy jego ruch przepadł (błąd sieci)
    if (t.botSecret && t.state.phase === 1) {
      const botIsA = t.state.a_id === t.botSecret.id;
      const botTurnNow = (t.state.turn === 0 && botIsA) || (t.state.turn === 1 && !botIsA);
      if (botTurnNow && !botPending.has(t.id)) {
        botPending.add(t.id);
        setTimeout(() => { botTurn(t.id).catch(() => {}).finally(() => botPending.delete(t.id)); }, 500);
      }
    }
    const { botSecret, ...safe } = t;          // plansza bota zostaje na serwerze
    // bot nigdy nie odchodzi od stołu; człowiek — bywa
    const online = { a: !!botSecret && t.state.a_id === botSecret.id ? true : isOnline(t.state.a_id),
                     b: !!botSecret && t.state.b_id === botSecret.id ? true : isOnline(t.state.b_id) };
    const nick = { a: botSecret && t.state.a_id === botSecret.id ? "BOT" : nickOf(t.state.a_id),
                   b: botSecret && t.state.b_id === botSecret.id ? "BOT" : nickOf(t.state.b_id) };
    return { table: { ...safe, vsBot: !!botSecret, online, nick } };
  },

  // Stół zakłada gracz A: genesis fundujemy my, bo A nie ma jeszcze nic na łańcuchu.
  "POST /api/table/create": async (b) => serialize(async () => {
    const now = Date.now();
    while (createLog.length && now - createLog[0].at > 3_600_000) createLog.shift();
    // Limity chronią portfel operatora przed pętlą, a nie przed graczami. Od v3
    // `rescue` odzyskuje depozyt stołu, więc realny koszt to same opłaty —
    // stąd luźniejsze progi. Za ciasny limit na IP odcinał całe biura i sieci
    // komórkowe (CGNAT), gdzie wielu graczy dzieli jeden adres.
    if (createLog.length >= 120) throw new Error("hourly limit of new tables reached — please try later");
    if (createLog.filter((c) => c.ip === b._ip).length >= 25) throw new Error("too many tables from this address — try again in an hour");
    createLog.push({ ip: b._ip, at: now });
    const w = await walletUtxo();
    const st = { a_id: b.playerId, b_id: ZERO32, root_a: b.root, root_b: ZERO32,
      turn: 0, pending: NO_PENDING, hits_a: 0, hits_b: 0,
      miss_a: 0, miss_b: 0, shots_a: 0, shots_b: 0,
      // v2: w fazie WAITING `deadline` to ZNACZNIK ZAŁOŻENIA STOŁU — dolna granica
      // locktime'u dla `join`. Cofnięty o 30 min, bo każdy ruch dostaje lockTime
      // 20 min wstecz (mediana czasu bloków wlecze się za zegarem).
      deadline: Date.now() - CREATE_BACKDATE,
      move_window: WINDOWS[b.window] ?? MOVE_WINDOW, phase: 0, winner: 0 };
    const r = runBuilder({ mode: "create", wallet: w, state: st, tableValue: TABLE_VALUE });
    const txid = await submit(r.tx);
    walletTip = r.walletAfter;
    const addr = kaspa.addressFromScriptPublicKey(new kaspa.ScriptPublicKey(0, r.tx.outputs[0].spkScript), NETWORK).toString();
    const d = db();
    d.tables.push({ id: randomUUID().slice(0, 8), name: b.name || "Stół", covenantId: r.covenantId,
      outpoint: r.outpoint, state: st, address: addr, genesisTxid: txid, created: Date.now(),
      log: [{ kind: "create", txid, at: Date.now(), by: "A" }] });
    save(d);
    broadcast(d.tables[d.tables.length - 1].id);
    const created = d.tables[d.tables.length - 1];
    log("stół założony", txid.slice(0, 12));
    if (b.vsBot) {
      // bot dostaje własny klucz i planszę; siada natychmiast
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
        log("bot usiadł do stołu", created.id);
      } catch (e) { log("bot nie usiadł:", (e.message ?? e).toString().slice(0, 90)); }
    }
    return { ok: true, txid, id: created.id, address: addr, vsBot: !!b.vsBot };
  }),

  // Ruch (join/play/timeout) w dwóch krokach: skrót do podpisu, potem podpis.
  "POST /api/move/prepare": async (b) => serialize(async () => {
    const d = db();
    const t = d.tables.find((x) => x.id === b.id);
    if (!t) throw new Error("no such table");
    // Te same reguły egzekwuje kontrakt — sprawdzamy je tu tylko po to, żeby
    // gracz dostał zrozumiały komunikat zamiast surowego odrzucenia przez sieć.
    if (b.mode === "shoot") {
      if (t.state.pending !== NO_PENDING)
        throw new Error("their shot must be confirmed first");
      const mask = t.state.turn === 0 ? t.state.shots_a : t.state.shots_b;
      if (shotTaken(mask, b.params.cell))
        throw new Error("that square has already been fired at — pick another");
    }
    const w = await walletUtxo();   // aktualny tip; commit sprawdzi, że się nie zmienił
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
    // Gracz podpisał skrót transakcji zbudowanej na KONKRETNYM UTXO portfela.
    // Jeśli inny ruch w międzyczasie ten UTXO wydał, podpis jest już nieważny —
    // sygnalizujemy STALE, a przeglądarka po prostu buduje i podpisuje od nowa.
    const w = await walletUtxo();
    if (p.job.wallet.txid !== w.txid || p.job.wallet.index !== w.index) throw new Error("STALE");
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

// timeout może wywołać każdy — bez podpisu gracza
routes["POST /api/move/timeout"] = async (b) => serialize(async () => {
  const d = db();
  const t = d.tables.find((x) => x.id === b.id);
  if (!t) throw new Error("no such table");
  if (t.state.phase !== 1) throw new Error("the game is not running");
  // Kontrakt porównuje locktime (u nas: 20 min wstecz, bo mediana czasu bloków
  // wlecze się za zegarem) z deadline — więc sieć uzna timeout dopiero ~20 min
  // po terminie. Bez tej bramki błąd skryptu byłby mylący.
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

// POTWIERDZENIE strzału rywala — jedno żądanie, bez rundy podpisywania,
// bo entry `answer` jest keyless. Wysyła je przeglądarka OBROŃCY automatycznie.
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

// STRAŻNIK TERMINÓW. Gracz, który czeka na odpowiedź porzuconej partii, nie
// powinien musieć nic klikać — po upływie terminu (plus zapas na medianę czasu
// bloków) sami zgłaszamy przekroczenie i partia kończy się jego zwycięstwem.
// Przy okazji zamyka to wyścig opisany w audycie: kto pierwszy zgłosi, ten wygrywa.
setInterval(async () => {
  let tables;
  try { tables = db().tables; } catch { return; }
  const netNow = Date.now() - 1_200_000;   // ta sama skala co lockTime i deadline
  // 1) porzucone partie — rozstrzygnij po terminie
  for (const t of tables.filter((t) => t.state.phase === 1 && t.state.deadline > 0 && netNow >= t.state.deadline + 30_000)) {
    try {
      const r = await routes["POST /api/move/timeout"]({ id: t.id });
      log(`strażnik: ${t.id} rozstrzygnięty po terminie → ${r.txid.slice(0, 12)}`);
    } catch (e) { log(`strażnik ${t.id}:`, (e.message ?? e).toString().slice(0, 80)); }
  }
  // 2) wolne stoły, do których nikt nigdy nie usiadł — kontrakt pozwala je
  //    zamknąć po STALE_MS od założenia (w fazie WAITING `deadline` to znacznik
  //    utworzenia), więc nie zostają na łańcuchu na zawsze
  for (const t of tables.filter((t) => t.state.phase === 0 && !t.rescued && netNow >= t.state.deadline + STALE_MS)) {
    try {
      const r = await routes["POST /api/table/rescue"]({ id: t.id });
      log(`strażnik: wolny stół ${t.id} wygasł i został zamknięty → ${r.txid.slice(0, 12)}`);
    } catch (e) { log(`strażnik (wygasły) ${t.id}:`, (e.message ?? e).toString().slice(0, 80)); }
  }
  // 3) rozegrane stoły — zamknij kowenant i odzyskaj wartość, żeby nie wisiał
  for (const t of tables.filter((t) => t.state.phase === 2 && !t.rescued && Date.now() - (t.log[t.log.length - 1]?.at ?? 0) > 60_000)) {
    try {
      const r = await routes["POST /api/table/rescue"]({ id: t.id });
      log(`strażnik: ${t.id} zamknięty, wartość odzyskana → ${r.txid.slice(0, 12)}`);
    } catch (e) { log(`strażnik (zamknięcie) ${t.id}:`, (e.message ?? e).toString().slice(0, 80)); }
  }
}, 60_000);

// Odzyskanie wartości stołu (v2). Keyless jak timeout — kowenant znika, 0.6 TKAS
// wraca do portfela operatora zamiast palić się na zawsze.
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
  ".wasm": "application/wasm", ".json": "application/json", ".svg": "image/svg+xml" };

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
      log("BŁĄD", key, (e.message ?? e).toString().slice(0, 160));
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
  // pliki statyczne
  let p = normalize(url.pathname === "/" ? "/index.html" : url.pathname).replace(/^(\.\.[/\\])+/, "");
  const file = join(`${HERE}/public`, p);
  if (!file.startsWith(`${HERE}/public`) || !existsSync(file)) { res.writeHead(404); res.end("nie ma"); return; }
  res.writeHead(200, { "content-type": MIME[extname(file)] ?? "application/octet-stream", "cache-control": "no-store" });
  res.end(readFileSync(file));
}).listen(PORT, "127.0.0.1", async () => {
  log(`Statki: http://127.0.0.1:${PORT} (sieć ${NETWORK})`);
  // rozgrzewka: połączenie z węzłem i UTXO portfela w tle, żeby PIERWSZY gracz
  // nie płacił kilkunastu sekund za zimny start
  try { const t = Date.now(); await walletUtxo();
    log(`portfel gotowy w ${((Date.now() - t) / 1000).toFixed(1)}s: ${(walletTip.value / 1e8).toFixed(2)} TKAS`); }
  catch (e) { log("rozgrzewka nieudana:", (e.message ?? e).toString().slice(0, 80)); }
});
