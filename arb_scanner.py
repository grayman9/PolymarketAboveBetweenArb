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
import csv
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

MIN_EDGE_CENTS = 1.0       # min edge in cents to log (0.5c = $0.005)
MIN_SIZE_USD = 5.0         # min deployable size to log
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
    legs: list[dict]          # [{description, token, side, avg_price, size, leg_cost_usd}]
    total_cost: float         # sum of avg prices per share (at max_size)
    edge_cents: float         # (1 - total_cost) * 100 at max_size
    best_edge_cents: float    # edge at top-of-book (best case, 1 share)
    max_size_usd: float       # max shares at min edge
    pnl_usd: float            # profit at max_size_usd
    coverage: str             # human-readable coverage description


@dataclass
class SimPosition:
    pair_key: str             # "ASSET|date" unique key
    asset: str
    event_date: str
    legs: list[dict]          # [{token, side, description, avg_price, size, leg_cost_usd}]
    shares: float
    entry_cost: float         # total USD paid
    entry_time: str           # ISO timestamp
    covering_tokens: list[str]  # token_ids for bid-side exit checks


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
# Fee calculation (crypto taker fees)
# ---------------------------------------------------------------------------
CRYPTO_FEE_RATE = 0.25
CRYPTO_FEE_EXPONENT = 2


def taker_fee_mult(price: float) -> float:
    """Return the fee multiplier at a given price.

    fee_usdc = shares * price * CRYPTO_FEE_RATE * (price * (1 - price)) ^ CRYPTO_FEE_EXPONENT

    On BUY: fee collected in shares -> net shares = ordered * (1 - fee_mult)
    On SELL: fee collected in USDC  -> net proceeds = gross * (1 - fee_mult)
    """
    return CRYPTO_FEE_RATE * (price * (1.0 - price)) ** CRYPTO_FEE_EXPONENT


# ---------------------------------------------------------------------------
# Arbitrage detection
# ---------------------------------------------------------------------------
def compute_leg_cost(ob_manager: OrderbookManager, token_id: str,
                     target_size: Optional[float] = None) -> tuple[float, float]:
    """
    Compute the cost to buy `target_size` NET shares at ask, including taker fees.
    Fees are collected in shares: from each level we order X shares but receive
    X * (1 - fee_mult) net shares.  So effective cost per net share is higher.
    Returns (avg_cost_per_net_share, net_shares_filled).
    If target_size is None, returns (best_ask_price_with_fee, best_ask_net_size).
    """
    book = ob_manager.books.get(token_id)
    if not book or not book.asks:
        return (float('inf'), 0.0)

    if target_size is None:
        best = book.asks[0]
        fm = taker_fee_mult(best.price)
        net_size = best.size * (1.0 - fm)
        eff_price = best.price / (1.0 - fm) if fm < 1.0 else float('inf')
        return (eff_price, net_size)

    # Walk the book
    total_cost = 0.0
    net_filled = 0.0

    for lvl in book.asks:
        fm = taker_fee_mult(lvl.price)
        net_per_share = 1.0 - fm  # net shares received per share ordered
        if net_per_share <= 0:
            continue
        # How many net shares do we still need?
        need = target_size - net_filled
        # Max net shares available from this level
        avail_net = lvl.size * net_per_share
        take_net = min(avail_net, need)
        # How many raw shares to order for take_net net shares
        raw_shares = take_net / net_per_share
        total_cost += raw_shares * lvl.price
        net_filled += take_net
        if net_filled >= target_size - 1e-9:
            break

    if net_filled < 1e-9:
        return (float('inf'), 0.0)
    return (total_cost / net_filled, net_filled)


def compute_leg_sell(ob_manager: OrderbookManager, token_id: str,
                     target_size: float) -> tuple[float, float]:
    """
    Compute net proceeds from selling `target_size` shares at bid, after taker fees.
    Fees are collected in USDC: net = gross * (1 - fee_mult).
    Returns (avg_net_price_per_share, filled_size).
    """
    book = ob_manager.books.get(token_id)
    if not book or not book.bids:
        return (0.0, 0.0)

    total_net_proceeds = 0.0
    filled = 0.0
    for lvl in book.bids:  # sorted descending by price
        can_fill = min(lvl.size, target_size - filled)
        fm = taker_fee_mult(lvl.price)
        total_net_proceeds += can_fill * lvl.price * (1.0 - fm)
        filled += can_fill
        if filled >= target_size - 1e-9:
            break

    if filled < 1e-9:
        return (0.0, 0.0)
    return (total_net_proceeds / filled, filled)


def _between_covers_range(between_markets: list[Market], range_low: float, range_high: float) -> bool:
    """Check that between markets continuously cover [range_low, range_high] with no gaps."""
    if range_low >= range_high:
        return False
    # Filter to markets within the range
    relevant = sorted(
        [m for m in between_markets if m.lower is not None and m.upper is not None
         and m.lower >= range_low - 0.01 and m.upper <= range_high + 0.01],
        key=lambda m: m.lower,
    )
    if not relevant:
        return False
    # First market must start at range_low
    if abs(relevant[0].lower - range_low) > 0.01:
        return False
    # Walk the chain: each market's upper must equal the next market's lower
    cursor = relevant[0].upper
    for m in relevant[1:]:
        if abs(m.lower - cursor) > 0.01:
            return False
        cursor = m.upper
    # Last market must end at range_high
    return abs(cursor - range_high) <= 0.01


def _fmt_price(val: float) -> str:
    """Format a dollar price with appropriate precision.

    Values >= 100 use no decimals ($68,000), smaller values keep 2 ($1.10).
    """
    if val >= 100:
        return f"${val:,.0f}"
    return f"${val:,.2f}"


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
    # Validate that between markets cover the full range from lowest to highest
    if _between_covers_range(btwn, lowest, highest):
        baseline = []
        if pair.below_market:
            baseline.append((pair.below_market.yes_token, "YES",
                             f"Below {_fmt_price(pair.below_market.threshold)} [between]"))
        for m in btwn:
            baseline.append((m.yes_token, "YES",
                             f"Between {_fmt_price(m.lower)}-{_fmt_price(m.upper)} [between]"))
        if pair.above_tail_market:
            baseline.append((pair.above_tail_market.yes_token, "YES",
                             f"Above {_fmt_price(pair.above_tail_market.threshold)} [between]"))
        coverings.append(baseline)

    # Config 2: Above_T YES for the top + between for the rest
    for t_high in aligned:
        if t_high > highest or t_high < lowest:
            continue
        if t_high not in above_map:
            continue
        # Between contracts must cover lowest..t_high
        if not _between_covers_range(btwn, lowest, t_high):
            continue
        legs = []
        # Bottom: between tail
        if pair.below_market:
            legs.append((pair.below_market.yes_token, "YES",
                         f"Below {_fmt_price(pair.below_market.threshold)} [between]"))
        # Middle: between contracts up to t_high
        for m in btwn:
            if m.lower is not None and m.lower < t_high:
                legs.append((m.yes_token, "YES",
                             f"Between {_fmt_price(m.lower)}-{_fmt_price(m.upper)} [between]"))
        # Top: above YES
        am = above_map[t_high]
        legs.append((am.yes_token, "YES",
                     f"Above {_fmt_price(t_high)} YES [above]"))
        coverings.append(legs)

    # Config 3: Above_T NO for the bottom + between for the rest
    for t_low in aligned:
        if t_low > highest or t_low < lowest:
            continue
        if t_low not in above_map:
            continue
        # Between contracts must cover t_low..highest
        if not _between_covers_range(btwn, t_low, highest):
            continue
        legs = []
        # Bottom: above NO
        am = above_map[t_low]
        legs.append((am.no_token, "NO",
                     f"Above {_fmt_price(t_low)} NO [above]"))
        # Middle: between contracts from t_low up
        for m in btwn:
            if m.lower is not None and m.lower >= t_low:
                legs.append((m.yes_token, "YES",
                             f"Between {_fmt_price(m.lower)}-{_fmt_price(m.upper)} [between]"))
        # Top: between tail
        if pair.above_tail_market:
            legs.append((pair.above_tail_market.yes_token, "YES",
                         f"Above {_fmt_price(pair.above_tail_market.threshold)} [between]"))
        coverings.append(legs)

    # Config 4: Above_T1 NO + between middle + Above_T2 YES
    for t_low, t_high in combinations(aligned, 2):
        if t_low >= t_high:
            continue
        if t_low not in above_map or t_high not in above_map:
            continue
        if t_low < lowest or t_high > highest:
            continue
        # Between contracts must cover t_low..t_high
        if not _between_covers_range(btwn, t_low, t_high):
            continue
        legs = []
        # Bottom: above NO
        am_low = above_map[t_low]
        legs.append((am_low.no_token, "NO",
                     f"Above {_fmt_price(t_low)} NO [above]"))
        # Middle: between contracts
        for m in btwn:
            if m.lower is not None and m.lower >= t_low and m.upper is not None and m.upper <= t_high:
                legs.append((m.yes_token, "YES",
                             f"Between {_fmt_price(m.lower)}-{_fmt_price(m.upper)} [between]"))
        # Top: above YES
        am_high = above_map[t_high]
        legs.append((am_high.yes_token, "YES",
                     f"Above {_fmt_price(t_high)} YES [above]"))
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

        for token_id, side, desc in covering:
            eff_price, net_size = compute_leg_cost(ob_manager, token_id)
            if eff_price == float('inf') or eff_price <= 0:
                valid = False
                break
            total_cost += eff_price
            min_size = min(min_size, net_size)

        if not valid or min_size < 1e-9:
            continue

        best_edge_cents = (1.0 - total_cost) * 100
        max_size_usd = min_size * (1.0 - total_cost) if total_cost < 1.0 else 0.0

        if best_edge_cents >= MIN_EDGE_CENTS and min_size >= MIN_SIZE_USD:
            # Second pass: compute max deployable size walking the book
            min_edge = MIN_EDGE_CENTS / 100  # e.g. 0.005

            def _test_size(sz):
                """Test if `sz` shares are feasible with at least min_edge.
                Returns (profitable: bool, total_cost: float)."""
                leg_costs = []
                for token_id, side, desc in covering:
                    avg_price, filled = compute_leg_cost(ob_manager, token_id, sz)
                    if filled < sz - 0.01:
                        return False, float('inf')
                    leg_costs.append(avg_price)
                tc = sum(leg_costs)
                return tc <= 1.0 - min_edge, tc

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

            if best_deploy < MIN_SIZE_USD or best_deploy_cost >= 1.0:
                continue

            # Compute per-leg details at best_deploy size
            deploy_legs = []
            for token_id, side, desc in covering:
                avg_price, filled = compute_leg_cost(ob_manager, token_id, best_deploy)
                leg_cost_usd = avg_price * best_deploy
                deploy_legs.append({
                    "description": desc,
                    "token": token_id,
                    "side": side,
                    "avg_price": round(avg_price, 6),
                    "size": best_deploy,
                    "leg_cost_usd": round(leg_cost_usd, 2),
                })

            total_cost_usd = sum(lg["leg_cost_usd"] for lg in deploy_legs)
            payout_usd = best_deploy  # covering guarantees exactly 1 share pays out
            pnl_usd = payout_usd - total_cost_usd

            coverage_desc = " + ".join(d for _, _, d in covering)
            n_above = sum(1 for _, _, d in covering if "[above]" in d)
            n_between = sum(1 for _, _, d in covering if "[between]" in d)

            opp = ArbOpportunity(
                timestamp=datetime.now(timezone.utc).isoformat(),
                event_date=pair.date_str,
                asset=pair.asset,
                legs=deploy_legs,
                total_cost=best_deploy_cost,
                edge_cents=(1.0 - best_deploy_cost) * 100,
                best_edge_cents=best_edge_cents,
                max_size_usd=best_deploy,
                pnl_usd=round(pnl_usd, 2),
                coverage=f"{n_above} above + {n_between} between legs: {coverage_desc}",
            )
            opportunities.append(opp)

    # Deduplicate coverings that span the same price range.
    # E.g. "Below $62k [between] + ..." and "Above $62k NO [above] + ..."
    # cover the same outcomes but use different tail tokens.
    # Keep only the best (lowest cost) per unique range.
    best_by_range: dict[str, ArbOpportunity] = {}
    for opp in opportunities:
        # Extract dollar thresholds from leg descriptions to form a range key
        prices = re.findall(r'\$[\d,]+(?:\.\d+)?', opp.coverage)
        range_key = f"{opp.asset}|{opp.event_date}|{'|'.join(sorted(set(prices)))}"
        prev = best_by_range.get(range_key)
        if prev is None or opp.total_cost < prev.total_cost:
            best_by_range[range_key] = opp

    deduped = list(best_by_range.values())
    deduped.sort(key=lambda x: -x.edge_cents)
    return deduped


# ---------------------------------------------------------------------------
# Trade simulator
# ---------------------------------------------------------------------------
class Simulator:
    """Paper-trades arb opportunities with realistic constraints.

    Rules:
    - One position per event pair (asset+date)
    - Picks the highest-PnL covering when multiple exist
    - Sells all legs when sum of bid prices >= $1/share (early exit)
    - Tracks realized P&L and capital deployed
    """

    def __init__(self):
        self.positions: dict[str, SimPosition] = {}  # pair_key -> position
        self.closed_trades: list[dict] = []
        self.total_pnl = 0.0
        self.csv_path = LOG_DIR / "sim_trades.csv"
        self._write_header()

    def _write_header(self):
        if not self.csv_path.exists():
            with open(self.csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "entry_time", "exit_time", "asset", "date", "action",
                    "shares", "entry_cost", "exit_proceeds", "pnl",
                    "n_legs", "leg_details",
                ])

    def check_entries(self, pairs: list, ob_manager: OrderbookManager):
        """For each asset without an open position, find best arb across all dates."""
        # Skip assets that already have an open position
        active_assets = {pos.asset for pos in self.positions.values()}

        # Collect best opportunity per asset across all dates
        best_per_asset: dict[str, tuple[ArbOpportunity, str]] = {}
        for pair in pairs:
            if pair.asset in active_assets:
                continue

            opps = check_arb(pair, ob_manager)
            for opp in opps:
                prev = best_per_asset.get(opp.asset)
                if prev is None or opp.pnl_usd > prev[0].pnl_usd:
                    pair_key = f"{opp.asset}|{opp.event_date}"
                    best_per_asset[opp.asset] = (opp, pair_key)

        for _, (opp, pair_key) in best_per_asset.items():
            self._enter(opp, pair_key)

    def _enter(self, opp: ArbOpportunity, pair_key: str):
        tokens = [lg["token"] for lg in opp.legs]
        pos = SimPosition(
            pair_key=pair_key,
            asset=opp.asset,
            event_date=opp.event_date,
            legs=opp.legs,
            shares=opp.max_size_usd,
            entry_cost=sum(lg["leg_cost_usd"] for lg in opp.legs),
            entry_time=opp.timestamp,
            covering_tokens=tokens,
        )
        self.positions[pair_key] = pos
        log.info(
            f"SIM ENTRY | {opp.asset} {opp.event_date} | "
            f"{opp.max_size_usd:.0f} shares | cost=${pos.entry_cost:.2f} | "
            f"expected_pnl=${opp.pnl_usd:.2f} | legs={len(opp.legs)}"
        )
        for lg in opp.legs:
            log.info(f"  BUY {lg['description']}: {lg['size']:.0f} @ {lg['avg_price']:.4f} = ${lg['leg_cost_usd']:.2f}")

        # Write BUY to CSV
        leg_details = " | ".join(
            f"{lg['description']}: {lg['size']:.0f} @ {lg['avg_price']:.4f} = ${lg['leg_cost_usd']:.2f}"
            for lg in opp.legs
        )
        with open(self.csv_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                opp.timestamp, "", opp.asset, opp.event_date, "BUY",
                f"{opp.max_size_usd:.0f}", f"{pos.entry_cost:.2f}",
                "", "", len(opp.legs), leg_details,
            ])

    def check_exits(self, ob_manager: OrderbookManager):
        """Check exits: sell when bids >= $1, or settle at expiry for $1/share."""
        to_close = []
        today = datetime.now(timezone.utc).date()
        months = ["january", "february", "march", "april", "may", "june",
                  "july", "august", "september", "october", "november", "december"]

        for pair_key, pos in self.positions.items():
            # Check expiry: event_date like "march 9" -> if today > that date, settle
            try:
                parts = pos.event_date.lower().split()
                month_idx = months.index(parts[0]) + 1
                day = int(parts[1])
                event_date = today.replace(month=month_idx, day=day)
                if today > event_date:
                    # Covering guarantees $1/share payout at expiry
                    to_close.append((pair_key, pos.shares, "SETTLE"))
                    continue
            except (ValueError, IndexError):
                pass

            # Check early exit: sell all legs when sum of bids >= $1/share
            total_bid_price = 0.0
            total_proceeds = 0.0
            can_sell = True

            for lg in pos.legs:
                avg_bid, filled = compute_leg_sell(ob_manager, lg["token"], pos.shares)
                if filled < pos.shares - 0.01:
                    can_sell = False
                    break
                total_bid_price += avg_bid
                total_proceeds += avg_bid * pos.shares

            if can_sell and total_bid_price >= 1.0:
                to_close.append((pair_key, total_proceeds, "SELL"))

        for pair_key, proceeds, action in to_close:
            self._exit(pair_key, proceeds, action)

    def _exit(self, pair_key: str, proceeds: float, action: str):
        pos = self.positions.pop(pair_key)
        pnl = proceeds - pos.entry_cost
        self.total_pnl += pnl
        now = datetime.now(timezone.utc).isoformat()

        log.info(
            f"SIM EXIT  | {pos.asset} {pos.event_date} | {action} | "
            f"{pos.shares:.0f} shares | proceeds=${proceeds:.2f} | "
            f"pnl=${pnl:.2f} | total_pnl=${self.total_pnl:.2f}"
        )

        leg_details = " | ".join(
            f"{lg['description']}: {lg['size']:.0f} @ {lg['avg_price']:.4f}"
            for lg in pos.legs
        )
        trade = {
            "entry_time": pos.entry_time, "exit_time": now,
            "asset": pos.asset, "date": pos.event_date, "action": action,
            "shares": pos.shares, "entry_cost": pos.entry_cost,
            "exit_proceeds": proceeds, "pnl": pnl,
        }
        self.closed_trades.append(trade)

        with open(self.csv_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                pos.entry_time, now, pos.asset, pos.event_date, action,
                f"{pos.shares:.0f}", f"{pos.entry_cost:.2f}",
                f"{proceeds:.2f}", f"{pnl:.2f}",
                len(pos.legs), leg_details,
            ])

    def summary(self):
        n_open = len(self.positions)
        n_closed = len(self.closed_trades)
        capital_deployed = sum(p.entry_cost for p in self.positions.values())
        log.info(
            f"SIM SUMMARY | open={n_open} (${capital_deployed:.2f} deployed) | "
            f"closed={n_closed} | realized_pnl=${self.total_pnl:.2f}"
        )


# ---------------------------------------------------------------------------
# Opportunity logging
# ---------------------------------------------------------------------------
def log_opportunity(opp: ArbOpportunity):
    """Log an arbitrage opportunity to file and console."""
    log.info(
        f"ARB FOUND | {opp.asset} {opp.event_date} | "
        f"best_edge={opp.best_edge_cents:.1f}c | edge@size={opp.edge_cents:.1f}c | "
        f"cost={opp.total_cost:.4f} | "
        f"size={opp.max_size_usd:.0f} shares | PnL=${opp.pnl_usd:.2f} | legs={len(opp.legs)}"
    )
    for leg in opp.legs:
        log.info(f"  {leg['description']}: avg={leg['avg_price']:.4f} x {leg['size']:.0f} = ${leg['leg_cost_usd']:.2f}")

    # Append to CSV
    csv_path = LOG_DIR / "opportunities.csv"
    write_header = not csv_path.exists()
    leg_details = " | ".join(
        f"{lg['description']}: {lg['size']:.0f} @ {lg['avg_price']:.4f} = ${lg['leg_cost_usd']:.2f}"
        for lg in opp.legs
    )
    total_cost_usd = sum(lg["leg_cost_usd"] for lg in opp.legs)
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "timestamp", "asset", "date", "best_edge_cents", "edge_at_size_cents",
                "cost_per_share", "shares", "total_cost_usd", "pnl_usd",
                "n_legs", "leg_details", "coverage",
            ])
        writer.writerow([
            opp.timestamp, opp.asset, opp.event_date,
            f"{opp.best_edge_cents:.2f}", f"{opp.edge_cents:.2f}",
            f"{opp.total_cost:.4f}",
            f"{opp.max_size_usd:.0f}", f"{total_cost_usd:.2f}", f"{opp.pnl_usd:.2f}",
            len(opp.legs), leg_details, opp.coverage,
        ])

    # Detailed JSON log
    json_path = LOG_DIR / "opportunities_detail.jsonl"
    with open(json_path, "a", encoding="utf-8") as f:
        record = {
            "timestamp": opp.timestamp,
            "asset": opp.asset,
            "event_date": opp.event_date,
            "best_edge_cents": round(opp.best_edge_cents, 2),
            "edge_at_size_cents": round(opp.edge_cents, 2),
            "total_cost": round(opp.total_cost, 4),
            "max_size_usd": round(opp.max_size_usd, 2),
            "pnl_usd": opp.pnl_usd,
            "legs": opp.legs,
            "coverage": opp.coverage,
        }
        f.write(json.dumps(record) + "\n")


def log_opportunity_duration(asset: str, event_date: str, first_seen: str,
                             gone_at: str, duration_s: float,
                             initial_edge_cents: float, initial_size: float,
                             peak_edge_cents: float, peak_size: float,
                             last_edge_cents: float, last_size: float,
                             coverage: str):
    """Log how long an arbitrage opportunity persisted with lifecycle stats."""
    csv_path = LOG_DIR / "opportunity_durations.csv"
    write_header = not csv_path.exists()
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "first_seen", "gone_at", "asset", "date",
                "duration_seconds",
                "initial_edge_cents", "initial_size_shares",
                "peak_edge_cents", "peak_size_shares",
                "last_edge_cents", "last_size_shares",
                "coverage",
            ])
        writer.writerow([
            first_seen, gone_at, asset, event_date,
            f"{duration_s:.1f}",
            f"{initial_edge_cents:.2f}", f"{initial_size:.0f}",
            f"{peak_edge_cents:.2f}", f"{peak_size:.0f}",
            f"{last_edge_cents:.2f}", f"{last_size:.0f}",
            coverage,
        ])


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
    sim = Simulator()
    reconnect_delay = 1
    last_sim_summary = time.time()

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
                # Track last logged arbs to avoid spamming: key -> (best_edge_cents, max_size)
                last_logged_arbs: dict[str, tuple[float, float]] = {}
                # Track arb lifecycle: key -> {first_seen_time, first_seen_iso, initial_edge, initial_size, peak_edge, peak_size}
                arb_lifecycle: dict[str, dict] = {}
                EDGE_CHANGE_THRESHOLD = 0.1  # re-log if edge changes by 0.1c
                SIZE_CHANGE_RATIO = 0.2      # re-log if size changes by 20%
                GONE_COOLDOWN_S = 10         # seconds before confirming arb is truly gone
                pending_gone: dict[str, float] = {}  # key -> time when first disappeared
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
                            # Backfill missing books via REST
                            missing = [t for t in token_list if t not in ob_manager.books or not ob_manager.books[t].asks]
                            if missing:
                                log.info(f"Backfilling {len(missing)} missing orderbooks via REST...")
                                ob_manager.fetch_initial(missing)
                                loaded = len(ob_manager.books)
                            log.info(f"Snapshots ready ({loaded}/{len(token_list)} books). Running first arb check...")
                            for p in pairs:
                                opps = check_arb(p, ob_manager)
                                if opps:
                                    for opp in opps:
                                        log_opportunity(opp)
                                else:
                                    log.info(f"  {p.asset} {p.date_str}: no arb at current prices")
                            last_data = time.time()  # reset after backfill so watchdog doesn't fire
                            log.info("Streaming orderbook updates... (Ctrl+C to stop)")
                        continue

                    # Throttle arb checks to every 50ms
                    if time.time() - last_check >= 0.05:
                        last_check = time.time()
                        active_keys = set()
                        for p in pairs:
                            opps = check_arb(p, ob_manager)
                            for opp in opps:
                                key = f"{opp.asset}|{opp.event_date}|{opp.coverage}"
                                active_keys.add(key)
                                # Cancel pending cooldown if arb is back
                                pending_gone.pop(key, None)

                                prev = last_logged_arbs.get(key)
                                if prev is None:
                                    log_opportunity(opp)
                                    last_logged_arbs[key] = (opp.best_edge_cents, opp.max_size_usd)
                                    if key not in arb_lifecycle:
                                        arb_lifecycle[key] = {
                                            "first_seen_time": time.time(),
                                            "first_seen_iso": opp.timestamp,
                                            "initial_edge": opp.best_edge_cents,
                                            "initial_size": opp.max_size_usd,
                                            "peak_edge": opp.best_edge_cents,
                                            "peak_size": opp.max_size_usd,
                                        }
                                else:
                                    prev_edge, prev_size = prev
                                    edge_changed = abs(opp.edge_cents - prev_edge) >= EDGE_CHANGE_THRESHOLD
                                    size_changed = prev_size > 0 and abs(opp.max_size_usd - prev_size) / prev_size >= SIZE_CHANGE_RATIO
                                    if edge_changed or size_changed:
                                        log_opportunity(opp)
                                        last_logged_arbs[key] = (opp.best_edge_cents, opp.max_size_usd)
                                    # Update peaks
                                    lc = arb_lifecycle.get(key)
                                    if lc:
                                        if opp.best_edge_cents > lc["peak_edge"]:
                                            lc["peak_edge"] = opp.best_edge_cents
                                        if opp.max_size_usd > lc["peak_size"]:
                                            lc["peak_size"] = opp.max_size_usd

                        # Mark newly disappeared arbs as pending (start cooldown)
                        newly_gone = set(last_logged_arbs) - active_keys
                        now_t = time.time()
                        for key in newly_gone:
                            if key not in pending_gone:
                                pending_gone[key] = now_t

                        # Confirm arbs that have been gone longer than cooldown
                        confirmed_gone = [
                            k for k, t in pending_gone.items()
                            if now_t - t >= GONE_COOLDOWN_S
                        ]
                        for key in confirmed_gone:
                            del pending_gone[key]
                            if key not in last_logged_arbs:
                                continue
                            asset, date, coverage = key.split("|", 2)
                            lc = arb_lifecycle.pop(key, None)
                            duration_s = time.time() - lc["first_seen_time"] if lc else 0.0
                            prev_edge, prev_size = last_logged_arbs[key]
                            log.info(
                                f"ARB GONE  | {asset} {date} | "
                                f"lasted {duration_s:.1f}s | "
                                f"peak_edge={lc['peak_edge']:.1f}c | peak_size={lc['peak_size']:.0f} | "
                                f"last_edge={prev_edge:.1f}c | last_size={prev_size:.0f}"
                                if lc else
                                f"ARB GONE  | {asset} {date} | "
                                f"last_edge={prev_edge:.1f}c | last_size={prev_size:.0f}"
                            )
                            log_opportunity_duration(
                                asset=asset, event_date=date,
                                first_seen=lc["first_seen_iso"] if lc else "",
                                gone_at=datetime.now(timezone.utc).isoformat(),
                                duration_s=duration_s,
                                initial_edge_cents=lc["initial_edge"] if lc else 0.0,
                                initial_size=lc["initial_size"] if lc else 0.0,
                                peak_edge_cents=lc["peak_edge"] if lc else 0.0,
                                peak_size=lc["peak_size"] if lc else 0.0,
                                last_edge_cents=prev_edge,
                                last_size=prev_size,
                                coverage=coverage,
                            )
                            del last_logged_arbs[key]

                        # Simulator: check exits first, then entries
                        sim.check_exits(ob_manager)
                        sim.check_entries(pairs, ob_manager)

                        # Periodic sim summary (every 5 min)
                        now_t = time.time()
                        if now_t - last_sim_summary >= 300:
                            last_sim_summary = now_t
                            sim.summary()

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
                eff_price, net_size = compute_leg_cost(ob_manager, token_id)
                if eff_price == float('inf'):
                    valid = False
                    break
                total_cost += eff_price
            if valid and total_cost < best_cost:
                best_cost = total_cost
                best_covering = covering

        if best_covering:
            edge = (1.0 - best_cost) * 100
            log.info(f"  Best covering cost: {best_cost:.4f} (edge: {edge:.1f}c, incl fees)")
            for token_id, side, desc in best_covering:
                eff_price, net_size = compute_leg_cost(ob_manager, token_id)
                price_str = f"{eff_price:.4f}" if eff_price < float('inf') else "N/A"
                size_str = f"{net_size:.1f}"
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
