"""
Polymarket Above/Between Arbitrage Scanner

Finds risk-free arbitrage by combining Above and Between markets
that cover all possible outcomes for less than $1.

Valid covering pattern:
  [Above_T1 NO]  +  [Between T1..T2 contracts]  +  [Above_T2 YES]
  (covers <T1)      (covers T1..T2 range)          (covers >=T2)

Either tail can be replaced by the Between event's own tail contract.
"""

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Optional

from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import websockets

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

MIN_EDGE_CENTS = 0.5       # min edge in cents to log (0.5c = $0.005)
MIN_SIZE_USD = 5.0          # min deployable size to log
SCAN_ASSETS = ["bitcoin", "ethereum", "solana", "xrp"]  # asset names for slug construction
SCAN_DAYS_AHEAD = 7        # how many days ahead to scan
REFRESH_INTERVAL = 3600    # re-discover events every hour (seconds)
STALE_TIMEOUT = 60         # force reconnect if no data for this many seconds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "scanner.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("arb")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class Market:
    question: str
    condition_id: str
    yes_token: str
    no_token: str
    market_type: str          # "between", "below", "above_tail", "above"
    lower: Optional[float]    # lower bound ($)  (None for below/above_tail)
    upper: Optional[float]    # upper bound ($)  (None for below/above_tail)
    threshold: Optional[float]  # for above markets


@dataclass
class OrderbookLevel:
    price: float
    size: float


@dataclass
class Orderbook:
    asks: list[OrderbookLevel] = field(default_factory=list)
    bids: list[OrderbookLevel] = field(default_factory=list)


@dataclass
class ArbOpportunity:
    timestamp: str
    event_date: str
    asset: str
    legs: list[dict]          # [{market, side, token, best_ask, ...}]
    total_cost: float         # sum of asks
    edge_cents: float         # (1 - total_cost) * 100
    max_size_usd: float       # limited by thinnest leg
    coverage: str             # human-readable coverage description


# ---------------------------------------------------------------------------
# Market discovery
# ---------------------------------------------------------------------------
def _fetch_event_by_slug(slug: str) -> Optional[dict]:
    r = requests.get(f"{GAMMA_URL}/events", params={"slug": slug}, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data[0] if data else None


def fetch_event_pairs_by_slug(assets: list[str], days_ahead: int) -> list[tuple[dict, dict, str, str]]:
    """Fetch above/between event pairs using slug patterns.

    Slug patterns:
      between: {asset}-price-on-{month}-{day}
      above:   {asset}-above-on-{month}-{day}

    Returns list of (between_event, above_event, asset, date_str).
    """
    from datetime import timedelta

    today = datetime.now(timezone.utc).date()
    months = ["january", "february", "march", "april", "may", "june",
              "july", "august", "september", "october", "november", "december"]

    slugs_to_fetch = []
    for asset in assets:
        for d in range(0, days_ahead + 1):
            date = today + timedelta(days=d)
            month_name = months[date.month - 1]
            day = date.day
            date_str = f"{month_name} {day}"
            between_slug = f"{asset}-price-on-{month_name}-{day}"
            above_slug = f"{asset}-above-on-{month_name}-{day}"
            slugs_to_fetch.append((asset, date_str, between_slug, above_slug))

    # Fetch all slugs concurrently
    results = {}
    all_slugs = []
    for asset, date_str, bs, as_ in slugs_to_fetch:
        all_slugs.append(bs)
        all_slugs.append(as_)

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_fetch_event_by_slug, s): s for s in all_slugs}
        for fut in as_completed(futures):
            slug = futures[fut]
            try:
                results[slug] = fut.result()
            except Exception:
                results[slug] = None

    pairs = []
    for asset, date_str, bs, as_ in slugs_to_fetch:
        between_ev = results.get(bs)
        above_ev = results.get(as_)
        if between_ev and above_ev:
            pairs.append((between_ev, above_ev, asset, date_str))

    return pairs


def parse_threshold(question: str) -> Optional[float]:
    """Extract dollar threshold from question text. Returns value in dollars."""
    # Match patterns like "$58,000", "$85,000.50", "$90000"
    m = re.search(r'\$([0-9,]+(?:\.\d+)?)', question)
    if m:
        return float(m.group(1).replace(",", ""))
    return None


def parse_between_bounds(question: str) -> tuple[Optional[float], Optional[float]]:
    """Extract lower and upper bounds from between question."""
    prices = re.findall(r'\$([0-9,]+(?:\.\d+)?)', question)
    if len(prices) >= 2:
        return float(prices[0].replace(",", "")), float(prices[1].replace(",", ""))
    return None, None


def classify_event(event: dict) -> Optional[str]:
    """Classify event as 'above' or 'between' type."""
    title = event.get("title", "").lower()
    # "Bitcoin above ___ on March 8?" -> above
    # "Bitcoin price on March 8?" -> between
    if "above" in title:
        return "above"
    elif "price" in title:
        return "between"
    return None


def extract_event_date(title: str) -> Optional[str]:
    """Extract date string from event title for matching."""
    # Match patterns like "March 8", "March 14", etc.
    m = re.search(r'((?:January|February|March|April|May|June|July|August|'
                  r'September|October|November|December)\s+\d{1,2})', title, re.I)
    if m:
        return m.group(1).lower()
    return None


def extract_asset(title: str) -> str:
    """Extract asset name from event title."""
    title_l = title.lower()
    if "bitcoin" in title_l or "btc" in title_l:
        return "BTC"
    if "ethereum" in title_l or "eth" in title_l:
        return "ETH"
    if "solana" in title_l or "sol" in title_l:
        return "SOL"
    return "UNKNOWN"


def parse_markets(event: dict, event_type: str) -> list[Market]:
    """Parse markets from an event into structured Market objects."""
    markets = []
    for m in event.get("markets", []):
        q = m.get("question", "")
        cid = m.get("conditionId", "")
        tokens_raw = m.get("clobTokenIds", "[]")
        if isinstance(tokens_raw, str):
            tokens = json.loads(tokens_raw)
        else:
            tokens = tokens_raw
        if len(tokens) < 2:
            continue
        if m.get("closed") or not m.get("acceptingOrders", True):
            continue
        yes_tok, no_tok = tokens[0], tokens[1]

        if event_type == "between":
            q_lower = q.lower()
            if "less than" in q_lower or "below" in q_lower:
                thresh = parse_threshold(q)
                markets.append(Market(
                    question=q, condition_id=cid,
                    yes_token=yes_tok, no_token=no_tok,
                    market_type="below",
                    lower=None, upper=thresh, threshold=thresh,
                ))
            elif "greater than" in q_lower or "above" in q_lower:
                thresh = parse_threshold(q)
                markets.append(Market(
                    question=q, condition_id=cid,
                    yes_token=yes_tok, no_token=no_tok,
                    market_type="above_tail",
                    lower=thresh, upper=None, threshold=thresh,
                ))
            elif "between" in q_lower:
                lo, hi = parse_between_bounds(q)
                markets.append(Market(
                    question=q, condition_id=cid,
                    yes_token=yes_tok, no_token=no_tok,
                    market_type="between",
                    lower=lo, upper=hi, threshold=None,
                ))
        elif event_type == "above":
            thresh = parse_threshold(q)
            markets.append(Market(
                question=q, condition_id=cid,
                yes_token=yes_tok, no_token=no_tok,
                market_type="above",
                lower=None, upper=None, threshold=thresh,
            ))
    return markets


# ---------------------------------------------------------------------------
# Event pairing
# ---------------------------------------------------------------------------
@dataclass
class EventPair:
    """A matched pair of above + between events for the same asset/date."""
    asset: str
    date_str: str
    between_markets: list[Market]   # sorted by threshold
    above_markets: list[Market]     # sorted by threshold
    below_market: Optional[Market]
    above_tail_market: Optional[Market]
    # Aligned thresholds where both events have contracts
    thresholds: list[float]
    # Maps: threshold -> Market for quick lookup
    between_by_lower: dict[float, Market] = field(default_factory=dict)
    above_by_thresh: dict[float, Market] = field(default_factory=dict)


def build_event_pair(between_ev: dict, above_ev: dict, asset: str, date_str: str) -> Optional[EventPair]:
    """Build an EventPair from a matched between/above event."""
    above_markets = parse_markets(above_ev, "above")
    between_raw = parse_markets(between_ev, "between")

    below_market = None
    above_tail = None
    between_markets = []
    for m in between_raw:
        if m.market_type == "below":
            below_market = m
        elif m.market_type == "above_tail":
            above_tail = m
        else:
            between_markets.append(m)

    between_markets.sort(key=lambda m: m.lower or 0)
    above_markets.sort(key=lambda m: m.threshold or 0)

    # Find aligned thresholds
    between_lowers = {m.lower for m in between_markets if m.lower}
    between_uppers = {m.upper for m in between_markets if m.upper}
    between_bounds = between_lowers | between_uppers
    if below_market and below_market.threshold:
        between_bounds.add(below_market.threshold)
    if above_tail and above_tail.threshold:
        between_bounds.add(above_tail.threshold)

    above_thresholds = {m.threshold for m in above_markets if m.threshold}
    aligned = sorted(between_bounds & above_thresholds)

    between_by_lower = {m.lower: m for m in between_markets if m.lower}
    above_by_thresh = {m.threshold: m for m in above_markets if m.threshold}

    return EventPair(
        asset=asset.upper(), date_str=date_str,
        between_markets=between_markets,
        above_markets=above_markets,
        below_market=below_market,
        above_tail_market=above_tail,
        thresholds=aligned,
        between_by_lower=between_by_lower,
        above_by_thresh=above_by_thresh,
    )


def discover_pairs() -> list[EventPair]:
    """Discover all event pairs using slug-based lookup."""
    raw_pairs = fetch_event_pairs_by_slug(SCAN_ASSETS, SCAN_DAYS_AHEAD)
    pairs = []
    for between_ev, above_ev, asset, date_str in raw_pairs:
        ep = build_event_pair(between_ev, above_ev, asset, date_str)
        if ep and ep.between_markets and ep.above_markets:
            pairs.append(ep)
    return pairs


def collect_tokens(pairs: list[EventPair]) -> set[str]:
    """Collect all token IDs from event pairs."""
    tokens = set()
    for p in pairs:
        if p.below_market:
            tokens.add(p.below_market.yes_token)
        if p.above_tail_market:
            tokens.add(p.above_tail_market.yes_token)
        for m in p.between_markets:
            tokens.add(m.yes_token)
        for m in p.above_markets:
            tokens.add(m.yes_token)
            tokens.add(m.no_token)
    return tokens


# ---------------------------------------------------------------------------
# Orderbook management
# ---------------------------------------------------------------------------
class OrderbookManager:
    """Maintains live orderbooks for all tracked tokens."""

    def __init__(self):
        self.books: dict[str, Orderbook] = {}  # token_id -> Orderbook
        self._lock = asyncio.Lock()

    def get_best_ask(self, token_id: str) -> Optional[OrderbookLevel]:
        book = self.books.get(token_id)
        if book and book.asks:
            return book.asks[0]
        return None

    def get_cumulative_asks(self, token_id: str, max_levels: int = 50) -> list[OrderbookLevel]:
        """Return asks as cumulative levels (price, cumulative_size)."""
        book = self.books.get(token_id)
        if not book or not book.asks:
            return []
        result = []
        cum_size = 0.0
        for lvl in book.asks[:max_levels]:
            cum_size += lvl.size
            result.append(OrderbookLevel(price=lvl.price, size=cum_size))
        return result

    def update_book(self, token_id: str, asks: list, bids: list):
        """Full snapshot replacement."""
        parsed_asks = sorted(
            [OrderbookLevel(float(a["price"]), float(a["size"])) for a in asks],
            key=lambda x: x.price
        )
        parsed_bids = sorted(
            [OrderbookLevel(float(b["price"]), float(b["size"])) for b in bids],
            key=lambda x: -x.price
        )
        self.books[token_id] = Orderbook(asks=parsed_asks, bids=parsed_bids)

    def apply_price_change(self, token_id: str, price: float, size: float, side: str):
        """Apply a single price-level delta. size=0 removes the level."""
        book = self.books.get(token_id)
        if book is None:
            book = Orderbook()
            self.books[token_id] = book

        if side == "SELL":
            levels = book.asks
            levels = [lvl for lvl in levels if abs(lvl.price - price) > 1e-9]
            if size > 0:
                levels.append(OrderbookLevel(price, size))
            book.asks = sorted(levels, key=lambda x: x.price)
        else:  # BUY
            levels = book.bids
            levels = [lvl for lvl in levels if abs(lvl.price - price) > 1e-9]
            if size > 0:
                levels.append(OrderbookLevel(price, size))
            book.bids = sorted(levels, key=lambda x: -x.price)

    def _fetch_one(self, tid: str):
        r = requests.get(f"{CLOB_URL}/book", params={"token_id": tid}, timeout=10)
        r.raise_for_status()
        data = r.json()
        return tid, data.get("asks", []), data.get("bids", [])

    def fetch_initial(self, token_ids: list[str], max_workers: int = 20):
        """Fetch orderbooks via REST concurrently."""
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self._fetch_one, tid): tid for tid in token_ids}
            done = 0
            for fut in as_completed(futures):
                done += 1
                if done % 50 == 0:
                    log.info(f"  Fetched {done}/{len(token_ids)} orderbooks...")
                try:
                    tid, asks, bids = fut.result()
                    self.update_book(tid, asks, bids)
                except Exception as e:
                    tid = futures[fut]
                    log.warning(f"Failed to fetch book for {tid[:20]}...: {e}")


# ---------------------------------------------------------------------------
# Arbitrage detection
# ---------------------------------------------------------------------------
def compute_leg_cost(ob_manager: OrderbookManager, token_id: str,
                     target_size: Optional[float] = None) -> tuple[float, float]:
    """
    Compute the cost to buy `target_size` shares at ask.
    Returns (avg_price_per_share, max_available_size).
    If target_size is None, returns (best_ask_price, best_ask_size).
    """
    asks = ob_manager.get_cumulative_asks(token_id)
    if not asks:
        return (float('inf'), 0.0)

    if target_size is None:
        return (asks[0].price, asks[0].size)

    # Walk the book
    total_cost = 0.0
    filled = 0.0
    book = ob_manager.books.get(token_id)
    if not book:
        return (float('inf'), 0.0)

    for lvl in book.asks:
        can_fill = min(lvl.size, target_size - filled)
        total_cost += can_fill * lvl.price
        filled += can_fill
        if filled >= target_size - 1e-9:
            break

    if filled < 1e-9:
        return (float('inf'), 0.0)
    return (total_cost / filled, filled)


def enumerate_coverings(pair: EventPair) -> list[list[tuple[str, str, str]]]:
    """
    Enumerate all valid coverings of the outcome space.

    Each covering is a list of (token_id, side, description) tuples where
    side is 'YES' or 'NO', indicating which token to buy.

    Returns list of coverings, each covering is a list of legs.
    """
    coverings = []

    # The between event defines the partition:
    # [below_thresh] [between_1] [between_2] ... [between_n] [above_tail_thresh]
    # Between markets sorted by lower bound
    btwn = pair.between_markets
    if not btwn:
        return []

    # The full range of between thresholds
    lowest = pair.below_market.threshold if pair.below_market else (btwn[0].lower if btwn else None)
    highest = pair.above_tail_market.threshold if pair.above_tail_market else (btwn[-1].upper if btwn else None)
    if lowest is None or highest is None:
        return []

    # Build ordered list of all boundaries
    boundaries = sorted(set(
        [lowest] +
        [m.lower for m in btwn if m.lower] +
        [m.upper for m in btwn if m.upper] +
        [highest]
    ))

    # Map: (lower_bound) -> between market
    btwn_map = pair.between_by_lower
    above_map = pair.above_by_thresh

    # Enumerate: pick a "cut_low" and "cut_high" threshold
    # - Everything below cut_low: covered by Above_cut_low NO (from above event)
    #   OR by between event's tail contracts
    # - Everything between cut_low and cut_high: covered by between contracts
    # - Everything above cut_high: covered by Above_cut_high YES (from above event)
    #   OR by between event's tail contracts

    # Aligned thresholds where we can cut
    aligned = pair.thresholds  # thresholds present in both events

    # Config 1: All between contracts (baseline)
    baseline = []
    if pair.below_market:
        baseline.append((pair.below_market.yes_token, "YES",
                         f"Below ${pair.below_market.threshold:,.0f} [between]"))
    for m in btwn:
        baseline.append((m.yes_token, "YES",
                         f"Between ${m.lower:,.0f}-${m.upper:,.0f} [between]"))
    if pair.above_tail_market:
        baseline.append((pair.above_tail_market.yes_token, "YES",
                         f"Above ${pair.above_tail_market.threshold:,.0f} [between]"))
    coverings.append(baseline)

    # Config 2: Above_T YES for the top + between for the rest
    for t_high in aligned:
        if t_high > highest or t_high < lowest:
            continue
        if t_high not in above_map:
            continue
        legs = []
        # Bottom: between tail
        if pair.below_market:
            legs.append((pair.below_market.yes_token, "YES",
                         f"Below ${pair.below_market.threshold:,.0f} [between]"))
        # Middle: between contracts up to t_high
        for m in btwn:
            if m.lower is not None and m.lower < t_high:
                legs.append((m.yes_token, "YES",
                             f"Between ${m.lower:,.0f}-${m.upper:,.0f} [between]"))
        # Top: above YES
        am = above_map[t_high]
        legs.append((am.yes_token, "YES",
                     f"Above ${t_high:,.0f} YES [above]"))
        coverings.append(legs)

    # Config 3: Above_T NO for the bottom + between for the rest
    for t_low in aligned:
        if t_low > highest or t_low < lowest:
            continue
        if t_low not in above_map:
            continue
        legs = []
        # Bottom: above NO
        am = above_map[t_low]
        legs.append((am.no_token, "NO",
                     f"Above ${t_low:,.0f} NO [above]"))
        # Middle: between contracts from t_low up
        for m in btwn:
            if m.lower is not None and m.lower >= t_low:
                legs.append((m.yes_token, "YES",
                             f"Between ${m.lower:,.0f}-${m.upper:,.0f} [between]"))
        # Top: between tail
        if pair.above_tail_market:
            legs.append((pair.above_tail_market.yes_token, "YES",
                         f"Above ${pair.above_tail_market.threshold:,.0f} [between]"))
        coverings.append(legs)

    # Config 4: Above_T1 NO + between middle + Above_T2 YES
    for t_low, t_high in combinations(aligned, 2):
        if t_low >= t_high:
            continue
        if t_low not in above_map or t_high not in above_map:
            continue
        if t_low < lowest or t_high > highest:
            continue
        legs = []
        # Bottom: above NO
        am_low = above_map[t_low]
        legs.append((am_low.no_token, "NO",
                     f"Above ${t_low:,.0f} NO [above]"))
        # Middle: between contracts
        for m in btwn:
            if m.lower is not None and m.lower >= t_low and m.upper is not None and m.upper <= t_high:
                legs.append((m.yes_token, "YES",
                             f"Between ${m.lower:,.0f}-${m.upper:,.0f} [between]"))
        # Top: above YES
        am_high = above_map[t_high]
        legs.append((am_high.yes_token, "YES",
                     f"Above ${t_high:,.0f} YES [above]"))
        coverings.append(legs)

    return coverings


def check_arb(pair: EventPair, ob_manager: OrderbookManager) -> list[ArbOpportunity]:
    """Check all coverings for arbitrage opportunities."""
    coverings = enumerate_coverings(pair)
    opportunities = []

    for covering in coverings:
        if len(covering) < 2:
            continue

        # First pass: best ask prices and min size
        total_cost = 0.0
        min_size = float('inf')
        valid = True
        leg_details = []

        for token_id, side, desc in covering:
            best = ob_manager.get_best_ask(token_id)
            if best is None or best.price <= 0:
                valid = False
                break
            total_cost += best.price
            min_size = min(min_size, best.size)
            leg_details.append({
                "description": desc,
                "token": token_id[:20] + "...",
                "side": side,
                "best_ask": best.price,
                "ask_size": best.size,
            })

        if not valid or min_size < 1e-9:
            continue

        edge_cents = (1.0 - total_cost) * 100
        max_size_usd = min_size * (1.0 - total_cost) if total_cost < 1.0 else 0.0

        if edge_cents >= MIN_EDGE_CENTS and min_size >= MIN_SIZE_USD:
            # Second pass: compute max deployable size walking the book
            def _test_size(sz):
                """Test if `sz` shares are feasible and profitable.
                Returns (profitable: bool, total_cost: float)."""
                leg_costs = []
                for token_id, side, desc in covering:
                    avg_price, filled = compute_leg_cost(ob_manager, token_id, sz)
                    if filled < sz - 0.01:
                        return False, float('inf')
                    leg_costs.append(avg_price)
                return sum(leg_costs) < 1.0, sum(leg_costs)

            test_sizes = [10, 50, 100, 250, 500, 1000, 2500, 5000]
            best_deploy = 0.0
            best_deploy_cost = total_cost
            upper_bound = None  # first size that fails

            for sz in test_sizes:
                profitable, tc = _test_size(sz)
                if profitable:
                    best_deploy = sz
                    best_deploy_cost = tc
                else:
                    upper_bound = sz
                    break

            # Binary search between last good tier and first bad tier
            if best_deploy > 0 and upper_bound is not None:
                lo, hi = best_deploy, upper_bound
                for _ in range(10):  # ~1-share precision
                    mid = (lo + hi) / 2
                    profitable, tc = _test_size(mid)
                    if profitable:
                        lo = mid
                        best_deploy = mid
                        best_deploy_cost = tc
                    else:
                        hi = mid
                best_deploy = int(best_deploy)  # round down to whole shares

            if best_deploy < MIN_SIZE_USD:
                continue

            coverage_desc = " + ".join(d for _, _, d in covering)
            n_above = sum(1 for _, _, d in covering if "[above]" in d)
            n_between = sum(1 for _, _, d in covering if "[between]" in d)

            opp = ArbOpportunity(
                timestamp=datetime.now(timezone.utc).isoformat(),
                event_date=pair.date_str,
                asset=pair.asset,
                legs=leg_details,
                total_cost=best_deploy_cost,
                edge_cents=(1.0 - best_deploy_cost) * 100,
                max_size_usd=best_deploy,
                coverage=f"{n_above} above + {n_between} between legs: {coverage_desc}",
            )
            opportunities.append(opp)

    # Sort by edge descending
    opportunities.sort(key=lambda x: -x.edge_cents)
    return opportunities


# ---------------------------------------------------------------------------
# Opportunity logging
# ---------------------------------------------------------------------------
def log_opportunity(opp: ArbOpportunity):
    """Log an arbitrage opportunity to file and console."""
    log.info(
        f"ARB FOUND | {opp.asset} {opp.event_date} | "
        f"edge={opp.edge_cents:.1f}c | cost={opp.total_cost:.4f} | "
        f"max_size=${opp.max_size_usd:.0f} | legs={len(opp.legs)}"
    )
    for leg in opp.legs:
        log.info(f"  {leg['description']}: ask={leg['best_ask']:.4f} size={leg['ask_size']:.1f}")

    # Append to CSV
    csv_path = LOG_DIR / "opportunities.csv"
    write_header = not csv_path.exists()
    with open(csv_path, "a", encoding="utf-8") as f:
        if write_header:
            f.write("timestamp,asset,date,edge_cents,total_cost,max_size_usd,n_legs,coverage\n")
        f.write(
            f"{opp.timestamp},{opp.asset},{opp.event_date},"
            f"{opp.edge_cents:.2f},{opp.total_cost:.4f},"
            f"{opp.max_size_usd:.0f},{len(opp.legs)},"
            f"\"{opp.coverage}\"\n"
        )

    # Detailed JSON log
    json_path = LOG_DIR / "opportunities_detail.jsonl"
    with open(json_path, "a", encoding="utf-8") as f:
        record = {
            "timestamp": opp.timestamp,
            "asset": opp.asset,
            "event_date": opp.event_date,
            "edge_cents": round(opp.edge_cents, 2),
            "total_cost": round(opp.total_cost, 4),
            "max_size_usd": round(opp.max_size_usd, 2),
            "legs": opp.legs,
            "coverage": opp.coverage,
        }
        f.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# WebSocket streaming
# ---------------------------------------------------------------------------
async def run_scanner():
    """Main scanner loop: discover events, stream orderbooks, detect arbs."""

    log.info("=" * 60)
    log.info("Polymarket Above/Between Arbitrage Scanner")
    log.info("=" * 60)

    # 1. Discover event pairs via slugs
    log.info("Discovering events...")
    pairs = discover_pairs()
    log.info(f"Found {len(pairs)} event pairs:")
    for p in pairs:
        n_cov = len(enumerate_coverings(p))
        log.info(f"  {p.asset} {p.date_str}: "
                 f"{len(p.between_markets)} between + {len(p.above_markets)} above markets, "
                 f"{n_cov} possible coverings")

    if not pairs:
        log.error("No event pairs found. Exiting.")
        return

    # 2. Collect all token IDs
    all_tokens = collect_tokens(pairs)
    token_list = list(all_tokens)
    log.info(f"Tracking {len(all_tokens)} tokens")

    ob_manager = OrderbookManager()
    reconnect_delay = 1

    while True:
        try:
            log.info("Connecting to WebSocket...")
            async with websockets.connect(WS_URL, ping_interval=None) as ws:
                # Subscribe to all tokens
                # Send in batches to avoid message size limits
                batch_size = 50
                for i in range(0, len(token_list), batch_size):
                    batch = token_list[i:i + batch_size]
                    sub_msg = json.dumps({
                        "assets_ids": batch,
                        "type": "market",
                        "custom_feature_enabled": True,
                    })
                    await ws.send(sub_msg)
                    log.info(f"  Subscribed to tokens {i+1}-{min(i+batch_size, len(token_list))}")

                reconnect_delay = 1
                last_check = time.time()
                last_heartbeat = time.time()
                last_refresh = time.time()
                last_data = time.time()
                snapshots_loaded = False
                snapshot_start = time.time()
                last_snapshot_log = 0  # track when we last logged snapshot progress

                log.info("Waiting for initial book snapshots...")

                while True:
                    # Heartbeat
                    now = time.time()
                    if now - last_heartbeat > 10:
                        await ws.send("PING")
                        last_heartbeat = now

                    # Staleness watchdog: force reconnect if no data for too long
                    if snapshots_loaded and now - last_data > STALE_TIMEOUT:
                        log.warning(f"No data received for {STALE_TIMEOUT}s, forcing reconnect...")
                        await ws.close()
                        break

                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=15)
                    except asyncio.TimeoutError:
                        await ws.send("PING")
                        last_heartbeat = time.time()
                        continue

                    if msg == "PONG":
                        continue

                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    # Initial subscription response is a list of book snapshots
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and item.get("event_type") == "book":
                                ob_manager.update_book(
                                    item.get("asset_id", ""),
                                    item.get("asks", []),
                                    item.get("bids", []),
                                )
                        last_data = time.time()
                    elif isinstance(data, dict):
                        evt_type = data.get("event_type")
                        if evt_type == "book":
                            ob_manager.update_book(
                                data.get("asset_id", ""),
                                data.get("asks", []),
                                data.get("bids", []),
                            )
                            last_data = time.time()
                        elif evt_type == "price_change":
                            for pc in data.get("price_changes", []):
                                ob_manager.apply_price_change(
                                    token_id=pc["asset_id"],
                                    price=float(pc["price"]),
                                    size=float(pc["size"]),
                                    side=pc["side"],
                                )
                            last_data = time.time()
                        else:
                            continue
                    else:
                        continue

                    # Check if we've loaded enough snapshots to start
                    if not snapshots_loaded:
                        loaded = len(ob_manager.books)
                        now = time.time()
                        # Log progress every 2 seconds
                        if now - last_snapshot_log >= 2:
                            last_snapshot_log = now
                            log.info(f"  Loaded {loaded}/{len(token_list)} books from WS snapshots")
                        # Start when 80% loaded OR after 15s timeout with any books
                        if (loaded >= len(token_list) * 0.8 or
                                (now - snapshot_start > 15 and loaded > 0)):
                            snapshots_loaded = True
                            log.info(f"Snapshots ready ({loaded}/{len(token_list)} books). Running first arb check...")
                            for p in pairs:
                                opps = check_arb(p, ob_manager)
                                if opps:
                                    for opp in opps:
                                        log_opportunity(opp)
                                else:
                                    log.info(f"  {p.asset} {p.date_str}: no arb at current prices")
                            log.info("Streaming orderbook updates... (Ctrl+C to stop)")
                        continue

                    # Throttle arb checks to every 50ms
                    if time.time() - last_check >= 0.05:
                        last_check = time.time()
                        for p in pairs:
                            opps = check_arb(p, ob_manager)
                            for opp in opps:
                                log_opportunity(opp)

                    # Hourly refresh: discover new events, sub/unsub tokens
                    if time.time() - last_refresh >= REFRESH_INTERVAL:
                        last_refresh = time.time()
                        log.info("Hourly refresh: re-discovering events...")
                        try:
                            new_pairs = discover_pairs()
                            new_tokens = collect_tokens(new_pairs)
                            old_tokens = set(token_list)

                            to_unsub = old_tokens - new_tokens
                            to_sub = new_tokens - old_tokens

                            if to_unsub:
                                for i in range(0, len(to_unsub), batch_size):
                                    batch = list(to_unsub)[i:i + batch_size]
                                    await ws.send(json.dumps({
                                        "assets_ids": batch,
                                        "type": "market",
                                        "operation": "unsubscribe",
                                    }))
                                for tid in to_unsub:
                                    ob_manager.books.pop(tid, None)
                                log.info(f"  Unsubscribed {len(to_unsub)} stale tokens")

                            if to_sub:
                                for i in range(0, len(to_sub), batch_size):
                                    batch = list(to_sub)[i:i + batch_size]
                                    await ws.send(json.dumps({
                                        "assets_ids": batch,
                                        "type": "market",
                                        "custom_feature_enabled": True,
                                    }))
                                log.info(f"  Subscribed to {len(to_sub)} new tokens")

                            pairs = new_pairs
                            token_list = list(new_tokens)
                            log.info(f"  Now tracking {len(pairs)} pairs, {len(token_list)} tokens")
                        except Exception as e:
                            log.warning(f"Hourly refresh failed: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"WebSocket disconnected: {e}. Reconnecting in {reconnect_delay}s...")
        except Exception as e:
            log.error(f"Error: {e}. Reconnecting in {reconnect_delay}s...")

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 60)

        # Refresh event data on reconnect (books will come from WS snapshots)
        try:
            pairs = discover_pairs()
            token_list = list(collect_tokens(pairs))
            ob_manager.books.clear()
        except Exception as e:
            log.warning(f"Failed to refresh events: {e}")


# ---------------------------------------------------------------------------
# One-shot mode (for testing without WebSocket)
# ---------------------------------------------------------------------------
def run_snapshot():
    """Run a single snapshot check (no streaming)."""
    log.info("=" * 60)
    log.info("Polymarket Arb Scanner - Snapshot Mode")
    log.info("=" * 60)

    pairs = discover_pairs()
    log.info(f"Found {len(pairs)} event pairs")

    if not pairs:
        log.info("No event pairs found.")
        return

    all_tokens = collect_tokens(pairs)

    ob_manager = OrderbookManager()
    log.info(f"Fetching {len(all_tokens)} orderbooks...")
    ob_manager.fetch_initial(list(all_tokens))

    total_opps = 0
    for p in pairs:
        coverings = enumerate_coverings(p)
        log.info(f"\n{p.asset} {p.date_str}: {len(coverings)} coverings to check")

        # Show best covering costs even without arb
        best_cost = float('inf')
        best_covering = None
        for covering in coverings:
            total_cost = 0.0
            valid = True
            for token_id, side, desc in covering:
                best = ob_manager.get_best_ask(token_id)
                if best is None:
                    valid = False
                    break
                total_cost += best.price
            if valid and total_cost < best_cost:
                best_cost = total_cost
                best_covering = covering

        if best_covering:
            edge = (1.0 - best_cost) * 100
            log.info(f"  Best covering cost: {best_cost:.4f} (edge: {edge:.1f}c)")
            for token_id, side, desc in best_covering:
                best = ob_manager.get_best_ask(token_id)
                price_str = f"{best.price:.4f}" if best else "N/A"
                size_str = f"{best.size:.1f}" if best else "N/A"
                log.info(f"    {desc}: ask={price_str} size={size_str}")

        opps = check_arb(p, ob_manager)
        total_opps += len(opps)
        for opp in opps:
            log_opportunity(opp)

    if total_opps == 0:
        log.info("\nNo arbitrage opportunities found at current prices.")
    else:
        log.info(f"\nFound {total_opps} arbitrage opportunities. See logs/ for details.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    if "--stream" in sys.argv:
        asyncio.run(run_scanner())
    else:
        run_snapshot()
