"""
HFT FEATURES v2 — Hardened + Tier 2 rollout for BigAl Sniper.
Drop-in upgrade for hft_features.py. All improvements + new capabilities.
"""
import asyncio, csv, difflib, math, os, random, time, statistics
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
import aiohttp

SOL_MINT = "So11111111111111111111111111111111111111112"
JUPITER_QUOTE_URL = "https://quote-api.jup.ag/v6/quote"
TOKEN_PROGRAM_ALLOWLIST = {"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA","TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"}
BC_GRADUATION_SOL = 85.0

class HoneypotGuard:
    def __init__(self, rpc_url: str, session: Optional[aiohttp.ClientSession] = None):
        self.rpc_url = rpc_url; self.session = session
        self._jup_cache: Dict[str, Tuple[bool, float]] = {}
        self._auth_cache: Dict[str, Tuple[bool, str, float]] = {}
        self.checks_total = 0; self.checks_blocked = 0
        self._reasons: Dict[str, int] = defaultdict(int)

    async def _sess(self):
        if self.session and not self.session.closed: return self.session
        try: self.session = aiohttp.ClientSession(); return self.session
        except: return None

    async def quick_check(self, mint: str) -> Tuple[bool, str]:
        self.checks_total += 1
        ok, r = await self._auth_check(mint)
        if not ok: self.checks_blocked += 1; self._reasons[r[:30]] += 1; return False, r
        ok, r = await self._jup_check(mint)
        if not ok: self.checks_blocked += 1; self._reasons["no_sell_route"] += 1; return False, r
        return True, ""

    async def _auth_check(self, mint: str) -> Tuple[bool, str]:
        now = time.time(); c = self._auth_cache.get(mint)
        if c and (now - c[2]) < (120 if not c[0] else 30): return c[0], c[1]
        s = await self._sess()
        if not s or not self.rpc_url or "YOUR_API_KEY" in self.rpc_url: return True, ""
        try:
            async with s.post(self.rpc_url, json={"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":[mint,{"encoding":"jsonParsed","commitment":"processed"}]}, timeout=aiohttp.ClientTimeout(total=1.8)) as resp:
                if resp.status != 200: return True, ""
                data = await resp.json()
            val = (data.get("result") or {}).get("value")
            if not val: return True, ""
            owner = val.get("owner", "")
            if owner and owner not in TOKEN_PROGRAM_ALLOWLIST:
                r = f"Custom program: {owner[:16]}"; self._auth_cache[mint] = (False, r, now); return False, r
            info = ((val.get("data") or {}).get("parsed") or {}).get("info") or {}
            if info.get("mintAuthority"):
                r = "Mint authority active"; self._auth_cache[mint] = (False, r, now); return False, r
            if info.get("freezeAuthority"):
                r = "Freeze authority active"; self._auth_cache[mint] = (False, r, now); return False, r
            self._auth_cache[mint] = (True, "", now); return True, ""
        except: return True, ""

    async def _jup_check(self, mint: str) -> Tuple[bool, str]:
        now = time.time(); c = self._jup_cache.get(mint)
        if c and (now - c[1]) < (60 if not c[0] else 15): return (c[0], "" if c[0] else "No sell route")
        s = await self._sess()
        if not s: return True, ""
        try:
            async with s.get(JUPITER_QUOTE_URL, params={"inputMint":mint,"outputMint":SOL_MINT,"amount":"1000000","slippageBps":"1500"}, timeout=aiohttp.ClientTimeout(total=2.0)) as resp:
                if resp.status == 400: self._jup_cache[mint] = (False, now); return False, "No sell route"
                if resp.status != 200: return True, ""
                data = await resp.json()
            ok = bool(data.get("routePlan")) and int(data.get("outAmount",0) or 0) > 0
            self._jup_cache[mint] = (ok, now); return (ok, "" if ok else "No sell route")
        except: return True, ""

    def prune(self):
        now = time.time()
        for k in [k for k,v in self._auth_cache.items() if now-v[2] > 120]: del self._auth_cache[k]
        for k in [k for k,v in self._jup_cache.items() if now-v[1] > 60]: del self._jup_cache[k]

    def get_stats(self) -> dict:
        return {"total":self.checks_total,"blocked":self.checks_blocked,"pass_rate":round((self.checks_total-self.checks_blocked)/max(1,self.checks_total)*100,1),"reasons":dict(self._reasons)}


@dataclass
class EntrySignal:
    name: str; score: float; reason: str; confidence: float = 1.0


class WhaleTracker:
    def __init__(self, min_wr=0.55, min_trades=8):
        self._mwr = min_wr; self._mt = min_trades
        self._w: Dict[str,dict] = {}; self._hot: Set[str] = set()
    def feed_result(self, wallet, pnl):
        if not wallet: return
        if wallet not in self._w: self._w[wallet] = {"w":0,"l":0,"p":0.0,"m":deque(maxlen=50)}
        d = self._w[wallet]
        if pnl >= 0: d["w"] += 1
        else: d["l"] += 1
        d["p"] += pnl; self._refresh()
    def feed_buy(self, wallet, mint):
        if not wallet: return
        if wallet not in self._w: self._w[wallet] = {"w":0,"l":0,"p":0.0,"m":deque(maxlen=50)}
        self._w[wallet]["m"].append(mint)
    def check(self, buyers: set) -> Tuple[int, float]:
        n = 0; rates = []
        for w in buyers:
            if w in self._hot:
                n += 1; d = self._w.get(w)
                if d:
                    t = d["w"]+d["l"]
                    if t > 0: rates.append(d["w"]/t)
        return n, (sum(rates)/len(rates) if rates else 0.0)
    def _refresh(self):
        self._hot = {w for w,d in self._w.items() if (d["w"]+d["l"]) >= self._mt and d["w"]/(d["w"]+d["l"]) >= self._mwr and d["p"] > 0}
    def count(self): return len(self._hot)


class InsiderTracker:
    def __init__(self, rpc_url="", session=None):
        self._rpc = rpc_url; self._sess = session; self._data: Dict[str,Dict[str,float]] = {}
    async def snapshot(self, mint, top_n=5):
        if not self._rpc or "YOUR_API_KEY" in self._rpc: return
        s = self._sess
        if not s or s.closed: return
        try:
            async with s.post(self._rpc, json={"jsonrpc":"2.0","id":1,"method":"getTokenLargestAccounts","params":[mint,{"commitment":"processed"}]}, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                if resp.status != 200: return
                data = await resp.json()
            accs = (data.get("result") or {}).get("value", [])
            tot = sum(float(a.get("uiAmount",0) or 0) for a in accs)
            h = {}
            for a in accs[:top_n]:
                addr = a.get("address",""); amt = float(a.get("uiAmount",0) or 0)
                pct = (amt/tot*100) if tot > 0 else 0
                if addr and pct > 1: h[addr] = pct
            if h: self._data[mint] = h
        except: pass
    def is_insider(self, mint, seller) -> Tuple[bool,float]:
        ins = self._data.get(mint, {}); return (True, ins[seller]) if seller in ins else (False, 0.0)
    def cleanup(self, active): 
        for m in [m for m in self._data if m not in active]: del self._data[m]
    def count(self) -> int:
        return len(self._data)


class EntryIntelligence:
    def __init__(self):
        self.whales = WhaleTracker()
        self.creators: Dict[str,deque] = defaultdict(lambda: deque(maxlen=64))
        self.bot_buys: Dict[str,List] = defaultdict(list)
        self.winners: deque = deque(maxlen=200)
        self._grad: Dict[str,float] = {}

    def on_token_create(self, creator):
        if creator: self.creators[creator].append(time.time())
    def on_buy(self, mint, wallet, is_bot_like=False):
        if wallet: self.whales.feed_buy(wallet, mint)
        if is_bot_like and wallet:
            self.bot_buys[mint].append((wallet, time.time(), 70.0))
            cutoff = time.time() - 60
            self.bot_buys[mint] = [(w,t,s) for w,t,s in self.bot_buys[mint] if t > cutoff]
    def on_winner(self, name, symbol, pnl_pct=0.0):
        if pnl_pct >= 8: self.winners.append((name.lower().strip(), symbol.lower().strip(), time.time()))
    def on_result(self, wallet, pnl_sol):
        self.whales.feed_result(wallet, pnl_sol)
    def update_grad(self, mint, vsol):
        if vsol >= 70: self._grad[mint] = vsol
        elif mint in self._grad and vsol < 60: del self._grad[mint]

    def score(self, mint, name, symbol, creator, buyer_count, buyer_wallets=None) -> Tuple[float, List[EntrySignal]]:
        sigs = []; now = time.time()
        wallet_set = set(buyer_wallets or [])
        wc, wr = self.whales.check(wallet_set)
        if wc > 0: sigs.append(EntrySignal("whale", 25*wc, f"{wc} whale(s) avg WR {wr:.0%}", min(1.0, wr+0.2)))
        if creator:
            recent = [t for t in self.creators[creator] if now-t < 3600]
            if len(recent) >= 8: sigs.append(EntrySignal("creator_spam", -35, f"{len(recent)} tokens/1h", 0.9))
            elif len([t for t in recent if now-t < 300]) >= 3: sigs.append(EntrySignal("creator_rapid", -20, "3+ tokens in 5min", 0.8))
        bots = {w for w,t,s in self.bot_buys.get(mint,[]) if now-t < 30}
        if len(bots) >= 2:
            scores = [s for w,t,s in self.bot_buys.get(mint,[]) if w in bots]
            avg = sum(scores)/len(scores) if scores else 50
            sigs.append(EntrySignal("consensus", 30, f"{len(bots)} bots (avg {avg:.0f})", min(1.0, avg/100+0.2)))
        if buyer_count >= 6: sigs.append(EntrySignal("momentum", 10, f"{buyer_count} buyers", 0.7))
        ln = name.lower().strip(); ls = symbol.lower().strip()
        for wn, ws, ts in reversed(self.winners):
            if now-ts > 3600: continue
            if len(wn) >= 3 and (wn in ln or ln in wn): sigs.append(EntrySignal("copycat", 15, f"matches '{wn}'", 0.85)); break
            if len(ws) >= 2 and (ws in ls or ls in ws): sigs.append(EntrySignal("copycat", 12, f"sym matches '{ws}'", 0.8)); break
            r = difflib.SequenceMatcher(None, wn, ln).ratio()
            if r >= 0.65: sigs.append(EntrySignal("copycat", 12, f"fuzzy '{wn}' ({r:.0%})", r)); break
        vs = self._grad.get(mint, 0)
        if vs >= 75: sigs.append(EntrySignal("graduation", 20, f"{vs/BC_GRADUATION_SOL*100:.0f}% to Raydium", 0.8))
        return round(sum(s.score*s.confidence for s in sigs), 1), sigs

    def prune(self):
        now = time.time()
        for c in list(self.creators):
            while self.creators[c] and now-self.creators[c][0] > 7200: self.creators[c].popleft()
            if not self.creators[c]: del self.creators[c]
        for m in list(self.bot_buys):
            self.bot_buys[m] = [(w,t,s) for w,t,s in self.bot_buys[m] if now-t < 120]
            if not self.bot_buys[m]: del self.bot_buys[m]
    def get_stats(self): return {"whales":self.whales.count(),"creators":len(self.creators),"consensus_mints":len(self.bot_buys),"winners":len(self.winners),"near_grad":len(self._grad)}


class MLExitPredictor:
    def __init__(self): self._stumps = []; self._ok = False; self._acc = 0.0; self._n = 0
    def train(self, path, n_stumps=25, lr=0.12):
        if not os.path.exists(path): return False
        try:
            with open(path) as f: rows = list(csv.DictReader(f))
        except: return False
        by_mint = defaultdict(list)
        for r in rows:
            m = r.get("mint","")
            if m: by_mint[m].append(r)
        X, y = [], []
        for mint, trades in by_mint.items():
            has_tp2 = any("TP2" in (t.get("reason","")) or "Trail" in (t.get("reason","")) for t in trades)
            for t in trades:
                reason = t.get("reason",""); feat = self._ext(t)
                if not feat: continue
                if "TP1" in reason: X.append(feat); y.append(1.0 if has_tp2 else -1.0)
                elif "SL" in reason or "Velocity" in reason: X.append(feat); y.append(-1.0)
        if len(X) < 15: return False
        self._n = len(X); n = len(X); w = [1.0/n]*n; self._stumps = []
        for _ in range(n_stumps):
            st = self._best(X, y, w)
            if not st: break
            preds = [self._sp(st,x) for x in X]
            err = max(1e-10, min(1-1e-10, sum(ww for i,ww in enumerate(w) if preds[i]!=y[i])))
            a = lr*0.5*math.log((1-err)/err); st["a"] = a
            for i in range(n): w[i] *= math.exp(-a*y[i]*preds[i])
            ws = sum(w); w = [ww/ws for ww in w]; self._stumps.append(st)
        if self._stumps: self._acc = sum(1 for i in range(n) if (self._raw(X[i])>0)==(y[i]>0))/n
        self._ok = bool(self._stumps); return self._ok

    def predict(self, pnl_pct, hold_sec, size_sol) -> Tuple[bool, float]:
        if not self._ok: return False, 0.5
        x = [pnl_pct, hold_sec, size_sol, pnl_pct*size_sol]
        conf = 1.0/(1.0+math.exp(-self._raw(x))); return conf > 0.6, round(conf, 3)
    def is_trained(self) -> bool:
        return self._ok

    def _ext(self, r):
        try:
            pnl = float(r.get("pnl_pct",0)); size = float(r.get("size_sol",0.02))
            try:
                from datetime import datetime
                hold = (datetime.fromisoformat(r["exit_time"])-datetime.fromisoformat(r["entry_time"])).total_seconds()
            except: hold = 10.0
            return [pnl, hold, size, pnl*size]
        except: return None
    def _best(self, X, y, w):
        best = None; be = float("inf")
        for fi in range(len(X[0])):
            vals = sorted(set(x[fi] for x in X))
            for i in range(len(vals)-1):
                th = (vals[i]+vals[i+1])/2
                for pol in [1,-1]:
                    preds = [1.0 if pol*x[fi]<=pol*th else -1.0 for x in X]
                    err = sum(ww for j,ww in enumerate(w) if preds[j]!=y[j])
                    if err < be: be = err; best = {"fi":fi,"th":th,"pol":pol}
        return best
    def _sp(self, s, x): return 1.0 if s["pol"]*x[s["fi"]] <= s["pol"]*s["th"] else -1.0
    def _raw(self, x): return sum(s["a"]*self._sp(s,x) for s in self._stumps)
    def info(self): return {"trained":self._ok,"samples":self._n,"stumps":len(self._stumps),"accuracy":round(self._acc,3)}


class GhostSeller:
    def __init__(self, min_sol=0.04, max_chunks=3): self._min = min_sol; self._mc = max_chunks
    def should(self, size): return size >= self._min
    def plan(self, total_pct=100.0, size=0.05):
        if not self.should(size): return [{"pct":total_pct,"delay":0}]
        n = min(self._mc, 2 if size < 0.08 else 3)
        sp = ([60,40] if n==2 else [50,30,20])
        sp = [s+random.uniform(-5,5) for s in sp]; t = sum(sp); sp = [s/t*total_pct for s in sp]
        return [{"pct":round(p,1),"delay":0 if i==0 else random.randint(400,800)} for i,p in enumerate(sp)]


class CounterIntel:
    def __init__(self, enabled=True, jmin=30, jmax=300, anoise=12.0, tnoise=15.0, skip_pct=8.0):
        self.enabled = enabled; self._jmin = jmin; self._jmax = jmax
        self._an = anoise/100; self._tn = tnoise/100; self._sp = skip_pct
        self._skipped = 0; self._eval = 0
    def delay_sec(self):
        if not self.enabled: return 0.0
        return min(self._jmax, self._jmin + random.expovariate(1.0/((self._jmax-self._jmin)*0.3)))/1000.0
    def adjust_size(self, base):
        if not self.enabled: return base
        return max(0.01, round(base+random.uniform(-base*self._an, base*self._an), 4))
    def adjust_tip(self, base):
        if not self.enabled: return base
        return max(0.0001, base+random.uniform(-base*0.08, base*0.08))
    def decoy_skip(self, buyers, volume):
        if not self.enabled: return False
        self._eval += 1; tb = (35 if buyers >= 5 else 0)+(35 if volume >= 1.5 else 0)
        if random.uniform(0,100) < self._sp*(1.5 if tb>=60 else 0.3): self._skipped += 1; return True
        return False
    def stats(self): return {"enabled":self.enabled,"eval":self._eval,"skipped":self._skipped}


class DynamicHold:
    def __init__(self, base=30.0, floor=10.0, ceil=90.0): self._b=base;self._f=floor;self._c=ceil
    def calc(self, buyers, volume, intel_score=0.0):
        h = self._b*(0.7+(min(buyers,8)-2)*0.1)*(0.8+min(volume,2.0)*0.2)
        if intel_score > 20: h *= 1.3
        elif intel_score < -10: h *= 0.6
        return max(self._f, min(self._c, round(h, 1)))


class VolumeExitSignal:
    def __init__(self, window=10.0, mult=3.0):
        self._w = window; self._m = mult; self._s: Dict[str,List] = defaultdict(list)
    def record(self, mint, sol):
        now = time.time(); self._s[mint].append((now, sol))
        self._s[mint] = [(t,a) for t,a in self._s[mint] if now-t < self._w*3]
    def is_spike(self, mint):
        now = time.time(); ev = self._s.get(mint, [])
        if len(ev) < 3: return False, 0.0
        rc = sum(a for t,a in ev if now-t < self._w); ol = sum(a for t,a in ev if now-t < self._w*2 and now-t >= self._w)
        rr = rc/self._w if self._w else 0; orr = ol/self._w if self._w else 0
        if orr > 0 and rr/orr > self._m: return True, round(rr,4)
        return False, round(rr,4)
    def cleanup(self, active):
        for m in [m for m in self._s if m not in active]: del self._s[m]
    def count(self) -> int:
        return len(self._s)


class BlockhashCache:
    def __init__(self, rpc_url, refresh_ms=400):
        self.rpc_url = rpc_url; self.refresh_ms = refresh_ms
        self.blockhash = None; self.fetched_at = 0.0
        self.running = False; self.session = None
        self.fetches = 0; self.errors = 0
    @property
    def age_ms(self):
        return (time.time()-self.fetched_at)*1000 if self.fetched_at else float("inf")
    async def start(self):
        if self.running or not self.rpc_url or "YOUR_API_KEY" in self.rpc_url: return
        self.running = True; self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2))
        asyncio.create_task(self._loop())
    async def stop(self):
        self.running = False
        if self.session and not self.session.closed: await self.session.close()
    async def force_refresh(self): await self._fetch(); return self.blockhash
    async def _loop(self):
        while self.running: await self._fetch(); await asyncio.sleep(self.refresh_ms/1000.0)
    async def _fetch(self):
        if not self.session or self.session.closed: return
        try:
            async with self.session.post(self.rpc_url, json={"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash","params":[{"commitment":"processed"}]}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    bh = ((data.get("result") or {}).get("value") or {}).get("blockhash")
                    if bh: self.blockhash = bh; self.fetched_at = time.time(); self.fetches += 1
        except: self.errors += 1
    def get_fresh(self, max_age=40.0):
        if self.blockhash and (time.time()-self.fetched_at) <= max_age: return self.blockhash
        return None
    def stats(self): return {"age_ms":round(self.age_ms,1),"fetches":self.fetches,"errors":self.errors}


class AnalyticsEngine:
    def __init__(self):
        self.fees = 0.0; self.pnl = 0.0; self.trades = 0
        self.session_pnl = 0.0; self.target = 0.5; self.floor = -0.3
        self._paused = False; self._reason = ""
        self._by_reason = defaultdict(lambda:{"n":0,"w":0,"p":0.0})
        self._by_hour = defaultdict(lambda:{"n":0,"w":0,"p":0.0})
        self._slips = deque(maxlen=200)

    def record_trade(self, pnl_sol, fee_sol=0.0, filter_tag="", **kwargs):
        reason = filter_tag or kwargs.get("reason", "")
        self.pnl += pnl_sol; self.fees += max(0,fee_sol); self.trades += 1; self.session_pnl += pnl_sol
        win = pnl_sol >= 0; h = time.localtime().tm_hour
        b = self._by_reason[reason[:20] or "unk"]; b["n"]+=1; b["p"]+=pnl_sol
        if win: b["w"]+=1
        hb = self._by_hour[h]; hb["n"]+=1; hb["p"]+=pnl_sol
        if win: hb["w"]+=1
        if self.session_pnl >= self.target: self._paused=True; self._reason=f"Target: {self.session_pnl:+.4f}"
        elif self.session_pnl <= self.floor: self._paused=True; self._reason=f"Floor: {self.session_pnl:+.4f}"

    def record_slippage(self, expected, realized, size=0.0):
        self._slips.append({"e":expected,"r":realized,"s":round(expected-realized,2),"sol":round(size*(expected-realized)/100,6)})

    def should_pause(self): return self._paused, self._reason
    def reset_session(self): self.session_pnl=0; self._paused=False; self._reason=""

    def report(self):
        ratio = 100.0 if self.pnl <= 0 else (self.fees/self.pnl)*100
        def _f(d):
            wr = d["w"]/d["n"]*100 if d["n"]>0 else 0
            return {"trades":d["n"],"wr":round(wr,1),"pnl":round(d["p"],4)}
        slips = [r["s"] for r in self._slips]
        return {
            "pnl":round(self.pnl,6),"session":round(self.session_pnl,6),"fees":round(self.fees,6),
            "fee_ratio":round(ratio,1),"trades":self.trades,"paused":self._paused,"reason":self._reason,
            "by_reason":{k:_f(v) for k,v in self._by_reason.items() if v["n"]>0},
            "by_hour":{f"{h:02d}":_f(v) for h,v in sorted(self._by_hour.items()) if v["n"]>0},
            "slippage":{"avg":round(statistics.mean(slips),2) if slips else 0,"total_sol":round(sum(r["sol"] for r in self._slips),6),"n":len(slips)},
        }
