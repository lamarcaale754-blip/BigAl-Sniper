/**
 * BigAl gRPC Sidecar — Helius LaserStream → ZMQ Bridge
 *
 * Streams Pump.fun program transactions via Helius LaserStream (sub-10ms latency)
 * and pushes parsed sightings to the Python bot via ZMQ IPC.
 *
 * Usage:
 *   HELIUS_API_KEY=your_key npm start
 *
 * Environment:
 *   HELIUS_API_KEY       — Required. Helius Pro API key.
 *   HELIUS_ENDPOINT      — Optional. LaserStream endpoint (default: US-East EWR).
 *   ZMQ_SIDECAR_ADDR     — Optional. ZMQ PUB address (default: ipc:///tmp/bigal_grpc_sidecar.ipc).
 *   ZMQ_SIDECAR_PORT     — Optional. TCP fallback port for Windows (default: 5558).
 *
 * Related: ../GRPC_RESOURCES.md (Yellowstone vs LaserStream vs Rust executor).
 */

import { subscribe, CommitmentLevel } from "helius-laserstream";
import { Publisher } from "zeromq";
import { Buffer } from "buffer";

// Pump.fun program ID
const PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

// Pump.fun instruction discriminators (first 8 bytes)
const DISC_CREATE = Buffer.from([24, 30, 200, 40, 5, 28, 7, 119]);
const DISC_BUY    = Buffer.from([102, 6, 61, 18, 1, 218, 235, 234]);
const DISC_SELL   = Buffer.from([51, 230, 133, 164, 1, 127, 131, 173]);

// ─────────────────────────────────────────────
//  Configuration
// ─────────────────────────────────────────────

const HELIUS_API_KEY = process.env.HELIUS_API_KEY;
if (!HELIUS_API_KEY) {
    console.error("[SIDECAR] HELIUS_API_KEY is required");
    process.exit(1);
}

const HELIUS_ENDPOINT = process.env.HELIUS_ENDPOINT ||
    "https://laserstream-mainnet-ewr.helius-rpc.com";

const isWindows = process.platform === "win32";
const ZMQ_ADDR = process.env.ZMQ_SIDECAR_ADDR ||
    (isWindows
        ? `tcp://127.0.0.1:${process.env.ZMQ_SIDECAR_PORT || 5558}`
        : "ipc:///tmp/bigal_grpc_sidecar.ipc");

// ─────────────────────────────────────────────
//  Instruction parsing helpers
// ─────────────────────────────────────────────

function matchDisc(ixData, disc) {
    if (!ixData || ixData.length < 8) return false;
    const buf = Buffer.from(ixData);
    return buf.subarray(0, 8).equals(disc);
}

function parsePumpIx(tx) {
    /**
     * Parse a Pump.fun transaction to extract:
     *  - txType: "create", "buy", or "sell"
     *  - mint: token mint address
     *  - trader: wallet that signed
     *  - solAmount: approximate SOL from balance changes
     *  - bonding curve data from account states
     */
    const meta = tx.meta;
    const message = tx.transaction?.message;
    if (!meta || !message) return null;

    const accountKeys = message.accountKeys || [];
    const instructions = message.instructions || [];

    // Find the Pump.fun instruction
    for (const ix of instructions) {
        const programIdx = ix.programIdIndex;
        if (programIdx >= accountKeys.length) continue;

        const programId = typeof accountKeys[programIdx] === "string"
            ? accountKeys[programIdx]
            : accountKeys[programIdx]?.pubkey;

        if (programId !== PUMP_FUN_PROGRAM) continue;

        const ixData = ix.data
            ? Buffer.from(ix.data, "base64")
            : null;

        if (!ixData) continue;

        let txType = null;
        if (matchDisc(ixData, DISC_CREATE)) txType = "create";
        else if (matchDisc(ixData, DISC_BUY)) txType = "buy";
        else if (matchDisc(ixData, DISC_SELL)) txType = "sell";
        else continue;

        // Account layout for Pump.fun:
        // Buy/Sell: [global, fee_recv, mint, bonding_curve, assoc_bc, user_ata, user, ...]
        // Create:   [mint, mint_authority, bonding_curve, assoc_bc, global, ...]
        const ixAccounts = ix.accounts || [];
        let mint = null;
        let trader = null;

        if (txType === "create") {
            mint = resolveAccount(accountKeys, ixAccounts[0]);
            trader = resolveAccount(accountKeys, ixAccounts[7]); // deployer
        } else {
            mint = resolveAccount(accountKeys, ixAccounts[2]);
            trader = resolveAccount(accountKeys, ixAccounts[6]);
        }

        // Extract SOL amount from balance changes
        const preBalances = meta.preBalances || [];
        const postBalances = meta.postBalances || [];
        let solAmount = 0;
        if (trader) {
            const traderIdx = accountKeys.findIndex(k =>
                (typeof k === "string" ? k : k?.pubkey) === trader
            );
            if (traderIdx >= 0 && traderIdx < preBalances.length) {
                solAmount = Math.abs(postBalances[traderIdx] - preBalances[traderIdx]) / 1e9;
            }
        }

        // Try to extract bonding curve state from account data
        let vSolInBondingCurve = 0;
        let vTokensInBondingCurve = 0;

        // The bonding curve account (index 3 for buy/sell) may have post-state
        const bcIdx = txType === "create" ? ixAccounts[2] : ixAccounts[3];
        if (meta.postTokenBalances && meta.postTokenBalances.length > 0) {
            // Approximate from token balances
            for (const tb of meta.postTokenBalances) {
                if (tb.mint === mint) {
                    const tokAmount = parseFloat(tb.uiTokenAmount?.uiAmountString || "0");
                    if (tokAmount > vTokensInBondingCurve) {
                        vTokensInBondingCurve = tokAmount;
                    }
                }
            }
        }

        // Approximate vSol from the bonding curve account SOL balance
        if (bcIdx !== undefined && bcIdx < postBalances.length) {
            vSolInBondingCurve = (postBalances[bcIdx] || 0) / 1e9;
        }

        return {
            txType,
            mint,
            traderPublicKey: trader,
            solAmount,
            vSolInBondingCurve,
            vTokensInBondingCurve,
            _source: "grpc",
        };
    }

    return null;
}

function resolveAccount(accountKeys, idx) {
    if (idx === undefined || idx >= accountKeys.length) return null;
    const key = accountKeys[idx];
    return typeof key === "string" ? key : key?.pubkey || null;
}

// ─────────────────────────────────────────────
//  Main
// ─────────────────────────────────────────────

let msgCount = 0;
let pushCount = 0;

async function startSidecar() {
    // Bind ZMQ PUB socket
    const sock = new Publisher();
    await sock.bind(ZMQ_ADDR);
    console.log(`[SIDECAR] ZMQ PUB bound: ${ZMQ_ADDR}`);

    const config = {
        apiKey: HELIUS_API_KEY,
        endpoint: HELIUS_ENDPOINT,
    };

    const request = {
        transactions: {
            "pump-fun-stream": {
                vote: false,
                failed: false,
                accountInclude: [PUMP_FUN_PROGRAM],
            },
        },
        commitment: CommitmentLevel.PROCESSED,
    };

    console.log(`[SIDECAR] Connecting to LaserStream: ${HELIUS_ENDPOINT}`);
    console.log(`[SIDECAR] Watching: ${PUMP_FUN_PROGRAM}`);

    await subscribe(config, request, async (update) => {
        msgCount++;

        try {
            const tx = update.transaction;
            if (!tx) return;

            const parsed = parsePumpIx(tx);
            if (!parsed || !parsed.mint) return;

            // Add slot and timestamp
            parsed.slot = update.slot || 0;
            parsed.timestamp_ms = Date.now();

            const payload = JSON.stringify(parsed);
            await sock.send(payload);
            pushCount++;

            if (pushCount % 100 === 0) {
                console.log(
                    `[SIDECAR] Pushed ${pushCount}/${msgCount} events | ` +
                    `last: ${parsed.txType} ${parsed.mint?.slice(0, 8)}...`
                );
            }
        } catch (err) {
            // Don't let a single parse error kill the stream
            if (msgCount % 1000 === 0) {
                console.warn(`[SIDECAR] Parse error (suppressed): ${err.message}`);
            }
        }
    });
}

// Graceful shutdown
process.on("SIGINT", () => {
    console.log(`\n[SIDECAR] Shutting down. Processed ${msgCount} msgs, pushed ${pushCount}`);
    process.exit(0);
});

startSidecar().catch((err) => {
    console.error(`[SIDECAR] Fatal: ${err.message}`);
    process.exit(1);
});
