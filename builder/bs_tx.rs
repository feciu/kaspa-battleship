// Transaction builder for Battleship: `prepare` mode returns the digest for the
// player's browser to sign, `finalize` mode assembles the final transaction with
// that signature. The table server never holds a player's key — the most it can
// do is refuse service.
use argent::build_file;
use argent_runtime::{ArgValue, ArtifactValue, EntryCall, InputSigScript, TxBuilder, TxContext, args, state};
use argent_template::{DemoResult, sign_input};
use kaspa_consensus_core::hashing::sighash::{SigHashReusedValuesUnsync, calc_schnorr_signature_hash};
use kaspa_consensus_core::hashing::sighash_type::SIG_HASH_ALL;
use kaspa_consensus_core::mass::{transaction_estimated_serialized_size, utxo_plurality};
use kaspa_consensus_core::tx::{CovenantBinding, ScriptPublicKey, Transaction, TransactionId, TransactionOutpoint, UtxoEntry};
use secp256k1::{Keypair, Secp256k1, SecretKey};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc, str::FromStr};

const C_STORAGE: u64 = 1_000_000_000_000;
/// Game moves carry a FIXED fee: the player signs a transaction digest, and the
/// digest depends on the change output, so the `prepare` and `finalize` phases
/// must arrive at the same amount. On testnet headroom is free, determinism is not.
const MOVE_FEE: u64 = 8_000_000;
fn hx(s: &serde_json::Value) -> Vec<u8> { hex::decode(s.as_str().unwrap()).unwrap() }

fn fee_for(tx: &Transaction) -> u64 {
    let size = transaction_estimated_serialized_size(tx);
    let spk: u64 = tx.outputs.iter().map(|o| 2 + o.script_public_key.script().len() as u64).sum();
    let budget: u64 = tx.inputs.iter().map(|i| i.compute_commit.compute_budget().unwrap_or(0) as u64).sum();
    let storage: u64 = tx.outputs.iter()
        .map(|o| utxo_plurality(&o.script_public_key, o.covenant.is_some()) * C_STORAGE / o.value.max(1)).sum();
    (size + spk * 10 + budget * 100).max(size * 4).max(storage) * 100 * 125 / 100
}

fn gstate(v: &serde_json::Value) -> BTreeMap<String, ArtifactValue> {
    let i = |k: &str| v[k].as_i64().unwrap();
    state! {
        a_id: hx(&v["a_id"]), b_id: hx(&v["b_id"]), root_a: hx(&v["root_a"]), root_b: hx(&v["root_b"]),
        turn: i("turn"), pending: i("pending"), hits_a: i("hits_a"), hits_b: i("hits_b"),
        miss_a: i("miss_a"), miss_b: i("miss_b"), shots_a: i("shots_a"), shots_b: i("shots_b"),
        deadline: i("deadline"), move_window: i("move_window"), phase: i("phase"), winner: i("winner") }
}

fn tx_json(tx: &Transaction, fee: u64) -> serde_json::Value {
    serde_json::json!({ "id": tx.id().to_string(), "fee": fee, "version": tx.version,
        "lockTime": tx.lock_time, "gas": tx.gas, "payload": hex::encode(&tx.payload),
        "inputs": tx.inputs.iter().map(|i| serde_json::json!({
            "txid": i.previous_outpoint.transaction_id.to_string(), "index": i.previous_outpoint.index,
            "signatureScript": hex::encode(&i.signature_script), "sequence": i.sequence,
            "computeCommit": format!("{:?}", i.compute_commit) })).collect::<Vec<_>>(),
        "outputs": tx.outputs.iter().map(|o| serde_json::json!({
            "value": o.value, "spkVersion": o.script_public_key.version(),
            "spkScript": hex::encode(o.script_public_key.script()),
            "covenant": o.covenant.as_ref().map(|c| serde_json::json!({
                "covenantId": c.covenant_id.to_string(), "authorizingInput": c.authorizing_input })) })).collect::<Vec<_>>() })
}

fn main() -> DemoResult<()> {
    let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("missing {k}"));
    let job: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(env("JOB"))?)?;
    let mode = job["mode"].as_str().unwrap();      // create | join | answer | shoot | timeout | rescue
    let prepare = job["prepare"].as_bool().unwrap_or(false);

    let sk = SecretKey::from_slice(&hex::decode(env("WALLET_PRIVKEY"))?)?;
    let kp = Keypair::from_secret_key(&Secp256k1::new(), &sk);
    let xonly = kp.x_only_public_key().0.serialize();
    let mut p2pk = vec![0x20u8]; p2pk.extend(xonly); p2pk.push(0xAC);
    let wspk = ScriptPublicKey::from_vec(0, p2pk.clone());

    let w = &job["wallet"];
    let wal_out = TransactionOutpoint { transaction_id: TransactionId::from_str(w["txid"].as_str().unwrap())?, index: w["index"].as_u64().unwrap() as u32 };
    let wal_val = w["value"].as_u64().unwrap();
    let net = env("NETWORK");   // testnet-10

    let artifact = build_file("ag/battleship3.ag", "build/battleship3")?;
    let b = TxBuilder::new(&artifact)?;
    let table_value: u64 = job["tableValue"].as_u64().unwrap_or(60_000_000);
    let lock = job["lockTime"].as_i64().unwrap_or(0);

    // the digest for the player to sign, captured while building
    let sighash: Rc<RefCell<Option<Vec<u8>>>> = Rc::new(RefCell::new(None));
    // the player's signature (finalize) or a dummy of the right length (prepare)
    let psig: Vec<u8> = if prepare { vec![0u8; 65] }
        else { job["playerSig"].as_str().map(|h| hex::decode(h).unwrap()).unwrap_or_default() };

    let wal_input = move |outp: TransactionOutpoint, val: u64| {
        (outp, UtxoEntry::new(val, ScriptPublicKey::from_vec(0, { let mut p = vec![0x20u8]; p.extend(xonly); p.push(0xAC); p }), 0, false, None),
         InputSigScript::with_transaction(move |tx: &_, i| { let s = sign_input(tx, i, &kp); let mut v = vec![s.len() as u8]; v.extend(s); v }))
    };

    if mode == "create" {
        let st = gstate(&job["state"]);
        let mk = |fee: u64| -> DemoResult<Transaction> {
            let (wo, wu, ws) = wal_input(wal_out, wal_val);
            Ok(b.build(&TxContext::new().input(wo, wu, ws, 0)
                .actor_genesis_output(0, "launch::main", "Game", st.clone(), table_value)
                .output(wspk.clone(), None, wal_val - table_value - fee))?) };
        let probe = mk(1_000_000)?;
        let fee = fee_for(&probe);
        let tx = mk(fee)?;
        let cov = tx.outputs[0].covenant.as_ref().unwrap().covenant_id;
        let addr_script = hex::encode(tx.outputs[0].script_public_key.script());
        println!("{}", serde_json::json!({ "tx": tx_json(&tx, fee), "covenantId": cov.to_string(),
            "outpoint": { "txid": tx.id().to_string(), "index": 0 },
            "walletAfter": { "txid": tx.id().to_string(), "index": 1, "value": wal_val - table_value - fee },
            "spk": addr_script, "network": net }));
        return Ok(());
    }

    // actor inputs: join / play / timeout / rescue
    let before = gstate(&job["state"]);
    let cov = kaspa_consensus_core::Hash::from_str(job["covenantId"].as_str().unwrap())?;
    let t_out = TransactionOutpoint { transaction_id: TransactionId::from_str(job["outpoint"]["txid"].as_str().unwrap())?, index: job["outpoint"]["index"].as_u64().unwrap() as u32 };

    // rescue: `emits none` — the covenant disappears and the table value returns to
    // the wallet. There is no actor output and no signature (the entry is keyless),
    // so there is no next state either — hence this branch sits before `nextState`.
    if mode == "rescue" {
        let tu = b.covenant_utxo("Game", before.clone(), table_value, 0, false, Some(cov))?;
        let (wo, wu, ws) = wal_input(wal_out, wal_val);
        let out_val = wal_val + table_value - MOVE_FEE;
        let tx = b.build(&TxContext::new().lock_time(lock as u64)
            .actor_input("Game", before.clone(), EntryCall::new("rescue"), t_out, tu, 0)
            .input(wo, wu, ws, 0)
            .output(wspk.clone(), None, out_val))?;
        println!("{}", serde_json::json!({ "tx": tx_json(&tx, MOVE_FEE),
            "walletAfter": { "txid": tx.id().to_string(), "index": 0, "value": out_val } }));
        return Ok(());
    }

    let after = gstate(&job["nextState"]);

    let p = &job["params"];
    let sh = sighash.clone();
    let mk_call = |sig: Vec<u8>| -> EntryCall<'static> {
        let (mode2, p2, sh2) = (mode.to_string(), p.clone(), sh.clone());
        match mode {
            "timeout" => EntryCall::new("timeout"),
            // The CONFIRMATION is keyless — the Merkle proof authenticates itself,
            // so there is neither a signature nor a digest to sign here.
            "answer" => EntryCall::new("answer").args_with(move |_tx, _idx| {
                let pr: Vec<ArgValue> = (0..7).map(|i| ArgValue::from(hx(&p2["proof"][i]))).collect();
                let mut a = args![p2["answer"].as_i64().unwrap(), hx(&p2["leafSalt"])];
                a.extend(pr);
                a
            }),
            _ => EntryCall::new(if mode2 == "join" { "join" } else { "shoot" }).args_with(move |tx, idx| {
                // the same digest the browser will sign (signatures stay out of the sighash)
                let reused = SigHashReusedValuesUnsync::new();
                let h = calc_schnorr_signature_hash(&tx.as_verifiable(), idx, SIG_HASH_ALL, &reused);
                *sh2.borrow_mut() = Some(h.as_bytes().to_vec());
                let pk = hx(&p2["pubkey"]);
                if mode2 == "join" {
                    args![hx(&p2["root"]), sig.clone(), pk]
                } else {
                    args![p2["cell"].as_i64().unwrap(), sig.clone(), pk]
                }
            }),
        }
    };

    let mk = |fee: u64, sig: Vec<u8>| -> DemoResult<Transaction> {
        let tu = b.covenant_utxo("Game", before.clone(), table_value, 0, false, Some(cov))?;
        let (wo, wu, ws) = wal_input(wal_out, wal_val);
        Ok(b.build(&TxContext::new().lock_time(lock as u64)
            .actor_input("Game", before.clone(), mk_call(sig), t_out, tu, 0)
            .input(wo, wu, ws, 0)
            .actor_output("Game", after.clone(), CovenantBinding::new(0u16, cov), table_value)
            .output(wspk.clone(), None, wal_val - fee))?) };

    if prepare {
        // building with a dummy signature fails in the script, but the digest is computed
        let _ = mk(MOVE_FEE, psig.clone());
        let h = sighash.borrow().clone().expect("sighash was not computed");
        println!("{}", serde_json::json!({ "sighash": hex::encode(h) }));
        return Ok(());
    }

    let fee = MOVE_FEE;
    let tx = mk(fee, psig)?;
    // The fixed fee is forced by sighash determinism, but it must cover the network
    // rate with room to spare — v2 state is larger than v1, so we check it here
    // explicitly instead of learning about it from a node rejection.
    let need = fee_for(&tx);
    if need > MOVE_FEE {
        return Err(format!("MOVE_FEE={MOVE_FEE} is too low, the network requires {need}").into());
    }
    println!("{}", serde_json::json!({ "tx": tx_json(&tx, fee),
        "outpoint": { "txid": tx.id().to_string(), "index": 0 },
        "walletAfter": { "txid": tx.id().to_string(), "index": 1, "value": wal_val - fee } }));
    Ok(())
}
