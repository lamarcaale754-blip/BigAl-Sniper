# BigAl Sniper — Filter Guide

A plain-English explanation of every slider in the dashboard.

---

## EXIT STRATEGY
*These control when the bot SELLS a token it already bought.*

### Stop Loss (sl)
**"How much am I willing to lose before cutting my losses?"**

If you buy a token and it drops this percentage, the bot sells immediately. Think of it like a safety net. At 15%, if you put in $1 and it drops to $0.85, the bot sells to prevent further loss.

*Lower = safer but more false alarms. Higher = more room but bigger losses when wrong.*

### Take Profit 1 (tp1)
**"When do I take some money off the table?"**

When a token goes up this percentage, the bot sells HALF your position. You lock in some profit but keep the other half riding in case it goes higher. At 12%, a $1 position that reaches $1.12 triggers a half-sell.

### Take Profit 2 (tp2)
**"When do I take the rest of my profit?"**

The second profit target for the remaining half. When hit, the bot sells almost everything (keeping a small moonbag if enabled). This is the "I'm happy with this gain" number.

### Max Hold Time (max_hold)
**"How long do I hold before giving up?"**

Maximum seconds the bot will hold a position. If a token hasn't hit any profit target or stop loss by this time, the bot sells at whatever price it's at. Prevents money from getting stuck in tokens that aren't moving.

---

## TRAILING STOP
*These lock in profits when a token is going up, by following the price upward.*

### Trail Trigger (trail_min)
**"How much profit before I start protecting my gains?"**

The trailing stop doesn't activate until the token reaches this profit level. At 8%, the trail sits dormant until your position is up 8%. Before that, only the regular stop loss protects you.

### Trail From Peak (trail_peak)
**"Once I'm in profit, how much pullback do I allow?"**

After the trail activates, this is how far the price can drop from its highest point before selling. Think of it like a dog on a leash following the price uphill — if the price turns around and walks back this far, the bot sells.

Example: Token peaks at +20%, trail is 4%. Bot sells when it drops to +16%.

*This is the most important profit-locking setting. Tighter = keeps more gains but might sell too early on a dip.*

---

## ENTRY FILTERS
*These control which tokens the bot BUYS. Every condition must be met.*

### Min Buyers (entry_buyers)
**"How many different people need to be buying before I jump in?"**

Each "buyer" is a unique wallet address. More buyers means real interest from multiple people, not just one person faking it. At 4, the bot needs 4 different wallets buying within the last 10 seconds.

### Min Buy Volume (entry_vol)
**"How much total money needs to be flowing in?"**

Total SOL spent on buys in the last 10 seconds. Higher = more real money behind the move. At 0.8 SOL, that's roughly $100-150 of buying pressure needed.

### Max Sell Ratio (sell_ratio)
**"How much selling am I okay with?"**

The percentage of total activity that's selling. If 50% of trades are sells, the token might be dumping. At 0.40 (40%), the bot skips any token where more than 40% of activity is people selling.

*Lower = pickier, only enters tokens where almost everyone is buying. Higher = more tolerant.*

### Min Net Buy (net_buy)
**"After subtracting sells from buys, is there still positive momentum?"**

Buy volume minus sell volume must exceed this. Even if there are lots of buys, if there are almost as many sells, the net momentum is weak. This ensures buys meaningfully outweigh sells.

### Max Pump (max_pump)
**"How much can it have already gone up before I'll still buy?"**

If a token already pumped 80% since creation, you're buying late — the early people might be about to sell. This prevents buying at the top. At 80%, the bot won't buy anything that's already up more than 80%.

### Min Token Age (min_age)
**"How long after a token is created before I'll consider buying?"**

Seconds to wait after a token appears. The first few seconds are chaos — bots sniping, creator selling, fake activity. Waiting lets the dust settle. At 4 seconds, the bot watches but won't buy for the first 4 seconds.

### Min Pool (min_pool)
**"What's the minimum liquidity I need to see?"**

The bonding curve pool must have at least this much SOL in it. Extremely low liquidity means your buy or sell could move the price dramatically (bad for you). This ensures there's enough liquidity for the bot to trade safely.

---

## BONDING CURVE
*PumpFun tokens have a "bonding curve" — a pool that starts at ~30 SOL. When it reaches ~85 SOL, the token "graduates" to a real exchange. These filters control WHEN in that journey the bot will buy.*

### BC Min Progress
**"How far along does the token need to be before I'll buy?"**

At 0%, the token just launched and nobody has bought it. At 100%, it's about to graduate. A very low BC means the token is unproven and more likely to be a scam. At 0.5%, the bot needs at least some buying activity before entering.

### BC Max Progress
**"How far along is too far — when am I too late?"**

If the bonding curve is already 22% of the way to graduation, the early buyers already made money and might dump on you. This prevents buying tokens that have already had their big move.

*Together, BC Min and BC Max create a "sweet spot" window where the token has proven interest but hasn't pumped too far.*

---

## MOONBAG
*When the bot sells a winner, it can keep a tiny piece as a "lottery ticket" in case the token skyrockets.*

### Moonbag %
**"What percentage of my winning position do I keep as a lottery ticket?"**

At 10%, when the bot sells a winner, it keeps 10% of the position. If you had $1 in a token and took profit, you keep $0.10 riding just in case it goes to the moon.

*Set to 0 to disable moonbags entirely.*

### Moonbag Expire
**"How long do I hold my lottery ticket?"**

Maximum seconds before the moonbag is automatically sold. Prevents tiny positions from sitting forever. At 120 seconds (2 minutes), the bot forces the moonbag closed if it hasn't hit any exit.

### Moonbag Stop Loss
**"How much can my lottery ticket lose before I cut it?"**

Since you already took profit on 90% of the position, you don't want the remaining 10% giving back those gains. At 15%, if the moonbag drops 15% from where it was created, the bot sells it.

### Moonbag Trail
**"How do I protect big moonbag gains?"**

A trailing stop specifically for moonbags. If your moonbag runs up to +200%, this prevents it from crashing back. At 20%, if it peaks at +200% and drops to +160%, the bot sells and locks in that gain.

---

## TIMING / EXPIRATION

### Candidate Max Age
**"How long do I watch a new token before giving up on it?"**

When a new token appears, the bot tracks it as a "candidate." If it doesn't meet entry conditions within this many seconds, the bot stops watching it and moves on. At 60 seconds, a token has one minute to prove itself.

---

## RISK / COOLDOWN

### Max Positions
**"How many tokens can I be in at the same time?"**

Limits how many open trades the bot has simultaneously. More positions = more chances to win but also more exposure to losses. At 10, the bot can hold 10 different tokens at once.

### Trade Size
**"How much SOL do I bet on each trade?"**

The amount of SOL spent on each entry. At 0.02 SOL (about $3-4), this is the maximum you can lose on a single trade (before stop loss). Keep this small while testing.

### Cooldown After
**"How many losses in a row before the bot takes a break?"**

If the bot loses this many trades in a row, it pauses new entries. This prevents spiraling during a bad streak. At 8, the bot needs to lose 8 consecutive trades before pausing.

### Cooldown Duration
**"How long does the bot pause after a losing streak?"**

Seconds the bot sits out after the cooldown triggers. At 10 seconds, it's a brief pause to let bad conditions pass.

---

## ANTI-MANIPULATION
*These detect fake activity designed to trick the bot into buying scam tokens.*

### Anti-Manipulation (on/off)
**"Should the bot try to detect fake trading patterns?"**

Turns the entire anti-manipulation system on or off. When ON, the bot checks for suspicious patterns before entering any trade.

### Wallet Diversity
**"How similar can buy amounts be before it looks suspicious?"**

If 5 people all buy exactly 0.1 SOL each, that's probably one person with 5 wallets faking activity. This measures how varied the buy amounts are. At 50%, at least half the buy amounts need to be different from each other.

### Max Repeat Wallets
**"How many times can the same wallet appear across different tokens?"**

If the same wallet keeps showing up as a "buyer" in token after token, it's likely a manipulation bot. At 3, if a wallet has appeared in 3 recent token entries, the bot skips any token where that wallet is a buyer.

### Random Skip
**"What percentage of qualifying trades should I randomly skip?"**

This makes the bot unpredictable. If someone watches the bot's wallet on-chain to figure out its exact filters, the random skip means they can never be 100% sure what triggers a buy. At 10%, the bot randomly passes on 1 out of every 10 qualifying trades.

*Higher = more unpredictable but misses more opportunities. 5-10% is a good balance.*
