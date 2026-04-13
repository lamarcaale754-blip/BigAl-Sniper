import WebSocket from "ws";
import { Publisher } from "zeromq";

const HELIUS_API_KEY = (process.env.HELIUS_API_KEY || "").trim();
if (!HELIUS_API_KEY) {
    console.error("[SIDECAR-WSS] HELIUS_API_KEY is required");
    process.exit(1);
}

const HELIUS_WSS = `wss://beta.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

const isWindows = process.platform === "win32";
const ZMQ_ADDR = process.env.ZMQ_SIDECAR_ADDR ||
    (isWindows
        ? `tcp://127.0.0.1:${process.env.ZMQ_SIDECAR_PORT || 5558}`
        : "ipc:///tmp/bigal_grpc_sidecar.ipc");

let ws = null;
let sock = null;
let reconnectTimer = null;

function resolvePubkey(k) {
    if (!k) return null;
    return typeof k === "string" ? k : k.pubkey || null;
}

function txAccountKeys(tx) {
    return tx?.transaction?.message?.accountKeys || [];
}

function resolveAccount(accountKeys, idx) {
    if (idx === undefined || idx === null || idx < 0 || idx >= accountKeys.length) return null;
    return resolvePubkey(accountKeys[idx]);
}

function parseIxData(ix) {
    if (!ix || !ix.data) return null;
    if (Array.isArray(ix.data)) return Buffer.from(ix.data);
    if (typeof ix.data === "string") {
        try { return Buffer.from(ix.data, "base64"); } catch (_) { return null; }
    }
    return null;
}

function matchDisc(ixData, bytes) {
    if (!ixData || ixData.length < 8) return false;
    for (let i = 0; i < 8; i += 1) {
        if (ixData[i] !== bytes[i]) return false;
    }
    return true;
}

const DISC_CREATE = [24, 30, 200, 40, 5, 28, 7, 119];
const DISC_BUY = [102, 6, 61, 18, 1, 218, 235, 234];
const DISC_SELL = [51, 230, 133, 164, 1, 127, 131, 173];

function detectType(logsText) {
    const logs = (logsText || "").toLowerCase();
    if (logs.includes("instruction: create") || logs.includes("create")) return "create";
    if (logs.includes("instruction: buy") || logs.includes("buy")) return "buy";
    if (logs.includes("instruction: sell") || logs.includes("sell")) return "sell";
    return null;
}

function parsePumpTx(tx, fallbackType) {
    const meta = tx?.meta || {};
    const message = tx?.transaction?.message || {};
    const accountKeys = txAccountKeys(tx);
    const instructions = message.instructions || [];
    const preBalances = meta.preBalances || [];
    const postBalances = meta.postBalances || [];
    const postTokenBalances = meta.postTokenBalances || [];

    let out = null;
    for (const ix of instructions) {
        const programId = resolveAccount(accountKeys, ix?.programIdIndex);
        if (programId !== PUMP_FUN) continue;
        const ixData = parseIxData(ix);

        let txType = fallbackType || null;
        if (matchDisc(ixData, DISC_CREATE)) txType = "create";
        else if (matchDisc(ixData, DISC_BUY)) txType = "buy";
        else if (matchDisc(ixData, DISC_SELL)) txType = "sell";
        if (!txType) continue;

        const ixAccounts = ix.accounts || [];
        const mint = txType === "create"
            ? resolveAccount(accountKeys, ixAccounts[0])
            : resolveAccount(accountKeys, ixAccounts[2]);
        const trader = txType === "create"
            ? resolveAccount(accountKeys, ixAccounts[7])
            : resolveAccount(accountKeys, ixAccounts[6]);
        if (!mint) continue;

        let solAmount = 0;
        if (trader) {
            const traderIdx = accountKeys.findIndex((k) => resolvePubkey(k) === trader);
            if (traderIdx >= 0 && traderIdx < preBalances.length && traderIdx < postBalances.length) {
                solAmount = Math.abs((postBalances[traderIdx] || 0) - (preBalances[traderIdx] || 0)) / 1e9;
            }
        }

        let tokenAmount = 0;
        let vTokensInBondingCurve = 0;
        for (const tb of postTokenBalances) {
            if (tb?.mint !== mint) continue;
            const raw = tb.uiTokenAmount?.uiAmountString ?? tb.uiTokenAmount?.uiAmount ?? "0";
            const parsed = Number.parseFloat(String(raw));
            if (Number.isFinite(parsed)) {
                tokenAmount += parsed;
                if (parsed > vTokensInBondingCurve) vTokensInBondingCurve = parsed;
            }
        }

        const bcIdx = txType === "create" ? ixAccounts[2] : ixAccounts[3];
        let vSolInBondingCurve = 0;
        if (bcIdx !== undefined && bcIdx < postBalances.length) {
            vSolInBondingCurve = (postBalances[bcIdx] || 0) / 1e9;
        }

        out = {
            txType,
            mint,
            traderPublicKey: trader,
            solAmount,
            tokenAmount,
            vSolInBondingCurve,
            vTokensInBondingCurve,
            slot: tx?.slot || 0,
            signature: tx?.transaction?.signatures?.[0] || null,
            timestamp_ms: Date.now(),
            source: "helius_gatekeeper_wss",
        };
        break;
    }
    return out;
}

async function pushEvent(payload) {
    await sock.send(JSON.stringify(payload));
}

function scheduleReconnect() {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(async () => {
        reconnectTimer = null;
        await connectWs();
    }, 500);
}

async function connectWs() {
    if (ws) {
        try { ws.terminate(); } catch (_) {}
        ws = null;
    }

    ws = new WebSocket(HELIUS_WSS);

    ws.on("open", () => {
        console.log("[SIDECAR-WSS] Helius Gatekeeper WSS online");
        ws.send(JSON.stringify({
            jsonrpc: "2.0",
            id: 1,
            method: "transactionSubscribe",
            params: [
                { accountInclude: [PUMP_FUN], failed: false },
                {
                    commitment: "processed",
                    encoding: "jsonParsed",
                    transactionDetails: "full",
                    maxSupportedTransactionVersion: 0,
                },
            ],
        }));
    });

    ws.on("message", async (data) => {
        try {
            const msg = JSON.parse(data.toString());
            const tx = msg?.params?.result;
            if (!tx) return;

            const logs = tx?.meta?.logMessages?.join(" ") || "";
            const txType = detectType(logs);
            if (!txType) return;
            const parsed = parsePumpTx(tx, txType);
            if (!parsed) return;
            await pushEvent(parsed);
        } catch (err) {
            console.warn(`[SIDECAR-WSS] parse/publish error: ${err.message}`);
        }
    });

    ws.on("close", () => {
        console.warn("[SIDECAR-WSS] websocket closed; reconnecting");
        scheduleReconnect();
    });

    ws.on("error", (err) => {
        console.warn(`[SIDECAR-WSS] websocket error: ${err.message}`);
    });
}

async function start() {
    sock = new Publisher();
    await sock.bind(ZMQ_ADDR);
    console.log(`[SIDECAR-WSS] ZMQ PUB bound: ${ZMQ_ADDR}`);
    await connectWs();
}

process.on("SIGINT", () => process.exit(0));
process.on("SIGTERM", () => process.exit(0));

start().catch((err) => {
    console.error(`[SIDECAR-WSS] fatal: ${err.message}`);
    process.exit(1);
});
