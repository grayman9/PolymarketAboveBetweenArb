# Above/Between Arbitrage Strategy

## The Opportunity

Polymarket lists two types of crypto price events for the same asset and date:

- **"Above" event**: e.g. "Bitcoin above $X on March 20?" — a set of YES/NO markets at different thresholds ($66k, $68k, $70k, ...)
- **"Between" event**: e.g. "Bitcoin price on March 20?" — a set of mutually exclusive range markets ($66k-$68k, $68k-$70k, ...) plus tail contracts (Below $X, Above $X)

Because these are separate events with separate orderbooks, their prices can temporarily diverge. When you can buy a set of contracts that **covers every possible outcome** for less than $1 total, you have a risk-free arbitrage: the payout is guaranteed to be exactly $1 (one contract always wins), so the profit is $1 minus cost.

## How Coverings Work

A "covering" is a combination of contracts from both events that together covers the entire price range with no gaps. There are four configurations:

### Config 1: All Between
Use the between event's own contracts to cover everything:
```
[Below $62k] + [Between $62k-$64k] + [Between $64k-$66k] + ... + [Above $80k]
   (between)       (between)              (between)                  (between)
```

### Config 2: Between bottom + Above YES top
Replace the between event's upper tail with an above YES:
```
[Below $62k] + [Between $62k-$64k] + ... + [Between $68k-$70k] + [Above $70k YES]
   (between)       (between)                    (between)            (above event)
```

### Config 3: Above NO bottom + Between top
Replace the between event's lower tail with an above NO:
```
[Above $66k NO] + [Between $66k-$68k] + ... + [Above $80k]
  (above event)       (between)                 (between)
```

### Config 4: Above NO bottom + Between middle + Above YES top
Use above event for both tails:
```
[Above $66k NO] + [Between $66k-$68k] + [Between $68k-$70k] + [Above $70k YES]
  (above event)       (between)              (between)           (above event)
```

**Critical constraint**: The between contracts MUST form a contiguous chain with no gaps. If between markets don't cover $70k-$72k, you can't use $70k and $72k as cut points — you'd have an uncovered range where all legs pay $0.

## Why This Works

Above and between events resolve independently but are priced by different market makers and liquidity pools. The mispricing is small (typically 1-3 cents per share) because:
- Fees eat into the edge (see below)
- Many participants are watching
- The arb is well-known

But with sufficient size (100-5000 shares), even 1-2 cent edges produce meaningful profit.

## Fee Model

Polymarket charges taker fees on crypto markets. Fees are highest at 50% probability and near-zero at extremes:

```
fee_multiplier = 0.25 * (price * (1 - price))^2
```

| Price | Effective Fee Rate |
|-------|-------------------|
| $0.01 | ~0.00% |
| $0.10 | 0.20% |
| $0.30 | 1.10% |
| $0.50 | **1.56%** (max) |
| $0.70 | 1.10% |
| $0.90 | 0.20% |
| $0.99 | ~0.00% |

**On BUY orders**: Fees are collected in shares. You order X shares but receive `X * (1 - fee_mult)` net shares. This means the effective cost per net share is `price / (1 - fee_mult)`.

**On SELL orders**: Fees are collected in USDC. Net proceeds = `gross * (1 - fee_mult)`.

Fees are a major factor — a covering that looks like $0.98 total cost before fees might be $0.995 after fees, cutting the edge from 2c to 0.5c.

## Execution Plan for Live Trading

### 1. Order Placement

All legs must be filled atomically or as close to it as possible. The risk window is the time between filling the first and last leg — if prices move during that window, the arb may no longer be profitable.

**Approach**: Use the [CLOB API](https://docs.polymarket.com) to place all leg orders simultaneously as market orders (or aggressive limit orders at the ask). Each leg is a BUY order for a specific token ID.

**Order types to consider**:
- **Market orders**: Fastest but no price protection. Risk of slippage if orderbook is thin.
- **Limit orders at the ask**: Price-protected but may not fill if ask moves. Place at the computed ask price and accept partial fills.
- **FOK (Fill or Kill)**: Ensures you get the full size or nothing. Best for atomicity but may have lower fill rates.

### 2. Size Calculation

The scanner's `check_arb` function already walks the orderbook to find the maximum deployable size where the edge stays above the minimum threshold. The key output is:
- `max_size_usd`: Number of shares to buy per leg (same for all legs)
- `total_cost`: Sum of average prices per share across all legs
- `edge_cents`: Profit per share in cents
- `best_edge_cents`: Top-of-book edge (best case, before walking the book)

**Start conservative**: Use a fraction of `max_size_usd` (e.g., 50%) to account for orderbook movement between detection and execution.

### 3. Exit Strategy

Since the covering guarantees exactly $1 payout at event resolution, there are two exit paths:

- **Hold to expiry**: Wait for the event to resolve. One leg wins, pays $1/share. Zero risk, but capital is locked until resolution (hours to days).
- **Sell early**: If the sum of bid prices across all legs reaches $1 or more, you can sell all legs for an immediate profit. This frees capital faster but requires monitoring bid-side liquidity.

### 4. Risk Considerations

| Risk | Mitigation |
|------|-----------|
| Partial fills (some legs fill, others don't) | Size conservatively; monitor fill status; have a cancel/unwind plan |
| Orderbook moves between detection and execution | Use the scanner's walked-book prices, not just top-of-book; add a safety margin |
| API rate limits / latency | Pre-compute orders; batch submissions; maintain persistent WebSocket connection |
| Event resolution disputes | Rare on crypto price markets — these resolve automatically from price feeds |
| Capital lockup | Prefer shorter-dated events (same day or next day) for faster turnover |

### 5. Capital Efficiency

From scanner data so far:
- Typical edge: 1-3 cents per share
- Typical size: 50-600 shares per opportunity
- Typical duration: Minutes (opportunities persist, not fleeting)
- Typical profit per trade: $0.50 - $6.00
- Assets scanned: BTC, ETH, SOL, XRP
- Dates scanned: 7 days ahead, refreshed hourly

This is a **high-frequency, low-margin** strategy. Profitability depends on:
1. Speed of execution (minimize risk window)
2. Volume (execute as many arbs as possible)
3. Capital turnover (exit early when possible, redeploy)

### 6. Maker vs Taker

The scanner currently assumes taker execution (hitting asks). An advanced implementation could:
- **Post limit orders** slightly below the ask to earn maker rebates (20% of fees redistributed daily)
- **Hybrid approach**: Take the most time-sensitive legs, make the less urgent ones
- Trade-off: Maker orders have fill uncertainty but reduce costs significantly

## Scanner Architecture

```
discover_pairs()          Slug-based lookup for above/between event pairs
    |
parse_markets()           Extract thresholds, bounds, token IDs
    |
build_event_pair()        Align thresholds between events
    |
enumerate_coverings()     Generate all valid covering configurations
    |                     (with gap validation via _between_covers_range)
    |
check_arb()              For each covering:
    |                      1. First pass: top-of-book prices + fees
    |                      2. Binary search: max size maintaining min edge
    |                      3. Dedup equivalent coverings by range
    |
WebSocket stream          Live orderbook updates -> re-check every 50ms
    |
log + track duration      Log opportunities, track lifecycle with cooldown
```

## Key Token Mapping

Each leg in a covering uses a specific **token ID** (a long hex string) that maps to one side of a market:
- `yes_token`: Pays $1 if the market resolves YES
- `no_token`: Pays $1 if the market resolves NO

For "Above $70k" from the above event:
- Buying YES = betting price >= $70k
- Buying NO = betting price < $70k

For "Between $68k-$70k" from the between event:
- Buying YES = betting price lands in that range

The scanner maps these automatically. The live implementation needs to use these exact token IDs when placing orders via the CLOB API.
