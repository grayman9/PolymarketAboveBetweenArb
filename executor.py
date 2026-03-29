"""
Live order executor for arb coverings.

Places FOK (Fill-or-Kill) orders for all legs of a covering.
Handles partial fills by verifying fills via API and unwinding if incomplete.
Uses live WebSocket orderbook data for exit pricing.
"""

import csv
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    BalanceAllowanceParams,
    OrderArgs,
    OrderType,
    PartialCreateOrderOptions,
)

load_dotenv()

log = logging.getLogger("arb")

LOG_DIR = Path(__file__).parent / "logs"
EXECUTIONS_CSV = LOG_DIR / "executions.csv"
POSITIONS_FILE = LOG_DIR / "open_positions.json"

# How long to wait for fills to settle before verifying
FILL_SETTLE_DELAY_S = 1.0
# Max retries when verifying order fill status
FILL_VERIFY_RETRIES = 3
FILL_VERIFY_DELAY_S = 1.0
# Max retries for unwind sell orders
UNWIND_RETRIES = 3
UNWIND_RETRY_DELAY_S = 2.0

# ---- Safety limits (defense-in-depth) ----
# Global rate limit: max orders placed per rolling window
MAX_ORDERS_PER_WINDOW = 20       # max orders in the window below
RATE_LIMIT_WINDOW_S = 60         # rolling window in seconds
# Circuit breaker: consecutive failures before halting all trading
CIRCUIT_BREAKER_THRESHOLD = 5
# Max open positions at any time
MAX_OPEN_POSITIONS = 5
# Cooldown after any execution failure
FAILURE_COOLDOWN_S = 60


# ---------------------------------------------------------------------------
# Cloudflare TLS fingerprint patch
# ---------------------------------------------------------------------------
def _patch_clob_http():
    """Monkey-patch py_clob_client to use curl_cffi with Chrome TLS fingerprint."""
    import py_clob_client.http_helpers.helpers as clob_helpers
    try:
        from curl_cffi import requests as cffi_requests
        _session = cffi_requests.Session(impersonate="chrome")
        _original_overload = clob_helpers.overloadHeaders

        def _cffi_request(endpoint, method, headers=None, data=None):
            headers = _original_overload(method, headers)
            if isinstance(data, str):
                resp = _session.request(method, endpoint, headers=headers,
                                        data=data.encode("utf-8"), timeout=30)
            else:
                resp = _session.request(method, endpoint, headers=headers,
                                        json=data if data else None, timeout=30)
            if resp.status_code != 200:
                raise clob_helpers.PolyApiException(resp)
            try:
                return resp.json()
            except Exception:
                return resp.text

        clob_helpers.request = _cffi_request
        log.info("Patched py_clob_client to use curl_cffi (Chrome TLS fingerprint)")
    except ImportError:
        log.warning("curl_cffi not installed — using default requests "
                     "(pip install curl_cffi if you hit Cloudflare blocks)")


# ---------------------------------------------------------------------------
# Token metadata cache
# ---------------------------------------------------------------------------
class TokenMetaCache:
    """Cache tick_size, neg_risk, fee_rate_bps per token to avoid repeated API calls."""

    def __init__(self, client: ClobClient, ttl: int = 3600):
        self._client = client
        self._ttl = ttl
        self._cache: dict[str, dict] = {}

    def get(self, token_id: str) -> dict:
        now = time.time()
        cached = self._cache.get(token_id)
        if cached and (now - cached["cached_at"]) < self._ttl:
            return cached
        meta = {
            "tick_size": self._client.get_tick_size(token_id),
            "neg_risk": self._client.get_neg_risk(token_id),
            "fee_rate_bps": self._client.get_fee_rate_bps(token_id),
            "cached_at": now,
        }
        self._cache[token_id] = meta
        log.info(f"Cached token meta for {token_id[:16]}... "
                 f"(tick={meta['tick_size']}, neg_risk={meta['neg_risk']}, "
                 f"fee_bps={meta['fee_rate_bps']})")
        return meta

    def prefetch(self, token_ids: list[str]):
        """Prefetch metadata for a batch of tokens."""
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(self.get, tid): tid for tid in token_ids}
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    log.warning(f"Failed to prefetch meta for "
                                f"{futures[fut][:16]}...: {e}")


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------
class ArbExecutor:
    """Places live orders for arb coverings.

    Entry flow:
      1. Place FOK buy orders for all legs concurrently
      2. Verify each fill via get_order() API
      3. Confirm share balances via get_balance_allowance()
      4. If any leg failed: unwind filled legs by selling shares back

    Exit flow:
      1. Detect exit via live WebSocket bids (ob_manager) — no REST call
      2. Verify share balances before selling
      3. Place FOK sell orders for all legs concurrently using ob_manager bids
      4. Confirm sells via get_order()
    """

    def __init__(self, max_trade_size: float):
        self.max_trade_size = max_trade_size
        self.client: ClobClient | None = None
        self.meta_cache: TokenMetaCache | None = None
        # Track open positions: pair_key -> position info
        self.positions: dict[str, dict] = {}
        # Cooldown: pair_key -> earliest retry time (monotonic)
        # Prevents rapid-fire retries after partial fills / unwind failures
        self._cooldown_until: dict[str, float] = {}
        self._csv_header_written = EXECUTIONS_CSV.exists()

        # --- Safety state ---
        # Global rate limiter: timestamps of all orders placed
        self._order_timestamps: list[float] = []
        # Circuit breaker: consecutive execution failures
        self._consecutive_failures = 0
        self._circuit_open = False

        self._load_positions()

    def initialize(self):
        """Initialize ClobClient from environment variables."""
        private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
        proxy_address = os.getenv("POLYMARKET_PROXY_ADDRESS")

        if not private_key or not proxy_address:
            raise RuntimeError(
                "Set POLYMARKET_PRIVATE_KEY and POLYMARKET_PROXY_ADDRESS in .env"
            )

        _patch_clob_http()

        self.client = ClobClient(
            "https://clob.polymarket.com",
            key=private_key,
            chain_id=137,
            signature_type=1,
            funder=proxy_address,
        )
        self.client.set_api_creds(self.client.create_or_derive_api_creds())

        if not self.client.get_ok():
            raise RuntimeError("ClobClient authentication failed")

        self.meta_cache = TokenMetaCache(self.client)
        log.info("ArbExecutor initialized (live mode)")
        if self.positions:
            log.info(f"Restored {len(self.positions)} open positions — reconciling balances...")
            self._reconcile_positions()

    # ------------------------------------------------------------------
    # Position persistence
    # ------------------------------------------------------------------
    def _save_positions(self):
        """Write open positions to disk as JSON."""
        try:
            with open(POSITIONS_FILE, "w", encoding="utf-8") as f:
                json.dump(self.positions, f, indent=2)
        except Exception as e:
            log.error(f"Failed to save positions: {e}")

    def _load_positions(self):
        """Load open positions from disk if the file exists."""
        if not POSITIONS_FILE.exists():
            return
        try:
            with open(POSITIONS_FILE, "r", encoding="utf-8") as f:
                self.positions = json.load(f)
        except Exception as e:
            log.error(f"Failed to load positions from {POSITIONS_FILE}: {e}")
            self.positions = {}

    def _reconcile_positions(self):
        """Verify restored positions still have shares on-chain.

        For each position, check that every leg's balance >= the bot's
        tracked size. If any leg has been manually closed (balance < size),
        remove the position — assume the operator handled it.
        """
        to_remove = []
        for pk, pos in self.positions.items():
            all_legs_ok = True
            for lg in pos["legs"]:
                balance = self._check_balance(lg["token_id"])
                if balance < pos["size"] - 0.01:
                    all_legs_ok = False
                    log.info(
                        f"  STALE | {pos['asset']} {pos['event_date']} | "
                        f"{lg.get('description', lg['token_id'][:16])} "
                        f"balance={balance:.1f} < tracked={pos['size']:.0f} — "
                        f"assuming manually closed"
                    )
                    break

            if all_legs_ok:
                log.info(
                    f"  OK    | {pos['asset']} {pos['event_date']} | "
                    f"{pos['size']:.0f} shares | cost=${pos['entry_cost']:.2f}"
                )
            else:
                to_remove.append(pk)

        if to_remove:
            for pk in to_remove:
                del self.positions[pk]
            self._save_positions()
            log.info(f"Removed {len(to_remove)} stale positions")

    # ------------------------------------------------------------------
    # Safety checks (defense-in-depth)
    # ------------------------------------------------------------------
    def _check_rate_limit(self, n_orders: int) -> bool:
        """Return True if placing n_orders would exceed the global rate limit."""
        now = time.monotonic()
        # Prune old timestamps outside the window
        cutoff = now - RATE_LIMIT_WINDOW_S
        self._order_timestamps = [t for t in self._order_timestamps if t > cutoff]
        if len(self._order_timestamps) + n_orders > MAX_ORDERS_PER_WINDOW:
            log.error(
                f"RATE LIMIT | {len(self._order_timestamps)} orders in last "
                f"{RATE_LIMIT_WINDOW_S}s + {n_orders} new > "
                f"{MAX_ORDERS_PER_WINDOW} limit — BLOCKING"
            )
            return True
        return False

    def _record_orders(self, count: int):
        """Record that `count` orders were placed (for rate limiting)."""
        now = time.monotonic()
        self._order_timestamps.extend([now] * count)

    def _record_success(self):
        """Reset consecutive failure count on a successful execution."""
        self._consecutive_failures = 0

    def _record_failure(self, pair_key: str):
        """Increment failure count and apply cooldown.

        If consecutive failures reach the threshold, open the circuit breaker
        which halts ALL trading until manually reset.
        """
        self._consecutive_failures += 1
        self._cooldown_until[pair_key] = time.monotonic() + FAILURE_COOLDOWN_S
        if self._consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
            self._circuit_open = True
            log.error(
                f"CIRCUIT BREAKER OPEN | {self._consecutive_failures} "
                f"consecutive failures — ALL TRADING HALTED. "
                f"Investigate and restart to resume."
            )

    def _pre_execute_checks(self, pair_key: str, n_legs: int) -> str | None:
        """Run all safety checks before execution.

        Returns None if all checks pass, or an error reason string.
        """
        if self._circuit_open:
            return "circuit breaker open"
        if pair_key in self.positions:
            return "position already open"
        if time.monotonic() < self._cooldown_until.get(pair_key, 0):
            return "cooldown active"
        if len(self.positions) >= MAX_OPEN_POSITIONS:
            return f"max positions ({MAX_OPEN_POSITIONS}) reached"
        if self._check_rate_limit(n_legs):
            return "rate limit"
        return None

    # ------------------------------------------------------------------
    # Order placement helpers
    # ------------------------------------------------------------------
    def _place_order(self, token_id: str, side: str, price: float,
                     size: float) -> dict:
        """Place a single FOK order (buy or sell). Returns result dict."""
        meta = self.meta_cache.get(token_id)
        order_args = OrderArgs(
            price=price,
            size=size,
            side=side,
            token_id=token_id,
            fee_rate_bps=meta["fee_rate_bps"],
        )
        order_options = PartialCreateOrderOptions(
            tick_size=meta["tick_size"],
            neg_risk=meta["neg_risk"],
        )
        signed_order = self.client.create_order(order_args, order_options)
        resp = self.client.post_order(signed_order, OrderType.FOK)

        order_id = ""
        success = False
        if isinstance(resp, dict):
            order_id = resp.get("orderID", "")
            success = bool(order_id)

        return {
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
            "response": resp,
            "success": success,
            "order_id": order_id,
        }

    # ------------------------------------------------------------------
    # Fill verification
    # ------------------------------------------------------------------
    def _verify_fill(self, order_id: str) -> dict | None:
        """Poll the API to confirm an order's fill status.

        Returns dict with status, size_matched, price or None on error.
        """
        for attempt in range(FILL_VERIFY_RETRIES):
            try:
                resp = self.client.get_order(order_id)
                if not resp:
                    time.sleep(FILL_VERIFY_DELAY_S)
                    continue

                order_info = resp
                if isinstance(resp, dict) and "order" in resp:
                    order_info = resp["order"]

                status = order_info.get("status", "")
                size_matched = float(order_info.get("size_matched", "0") or "0")
                price = float(order_info.get("price", "0") or "0")

                return {
                    "status": status,
                    "size_matched": size_matched,
                    "price": price,
                }
            except Exception as e:
                log.warning(f"  Fill verify attempt {attempt + 1} failed for "
                            f"{order_id}: {e}")
                if attempt < FILL_VERIFY_RETRIES - 1:
                    time.sleep(FILL_VERIFY_DELAY_S)

        return None

    def _check_balance(self, token_id: str) -> float:
        """Check how many shares we hold for a given token.

        Returns balance as float, 0.0 on error.
        """
        try:
            params = BalanceAllowanceParams(
                asset_type="CONDITIONAL",
                token_id=token_id,
            )
            result = self.client.get_balance_allowance(params=params)
            if isinstance(result, dict):
                return float(result.get("balance", "0") or "0")
            return 0.0
        except Exception as e:
            log.warning(f"  Balance check failed for {token_id[:16]}...: {e}")
            return 0.0

    # ------------------------------------------------------------------
    # Entry: buy all legs
    # ------------------------------------------------------------------
    def execute_opportunity(self, opp, pair_key: str) -> bool:
        """Execute an arb by placing FOK buy orders for all legs.

        Steps:
          0. Pre-execute safety checks (circuit breaker, rate limit, cooldown, etc.)
          1. Place FOK BUY for each leg concurrently
          2. Wait briefly for settlement
          3. Verify each fill via get_order()
          4. Confirm share balances
          5. If incomplete: unwind any filled legs

        Returns True if all legs confirmed filled.
        """
        n_legs = len(opp.legs)
        size = min(opp.max_size_usd, self.max_trade_size)
        if size < 1:
            return False

        # Run ALL safety checks before touching the exchange
        block_reason = self._pre_execute_checks(pair_key, n_legs)
        if block_reason:
            return False

        # === CRITICAL SECTION ===
        # Any unhandled exception MUST trigger _record_failure so the pair
        # goes on cooldown and the circuit breaker can trip. Without this,
        # a crash between order placement and failure recording would allow
        # the next cycle to retry immediately — the exact bug that caused
        # the order spam incident.
        try:
            return self._execute_inner(opp, pair_key, size, n_legs)
        except Exception as e:
            log.error(
                f"EXEC CRASH | {opp.asset} {opp.event_date} | "
                f"{type(e).__name__}: {e}"
            )
            self._record_failure(pair_key)
            return False

    def _execute_inner(self, opp, pair_key: str, size: float,
                       n_legs: int) -> bool:
        """Inner execution logic, wrapped by execute_opportunity's safety net."""
        legs = opp.legs
        log.info(
            f"EXEC BUY   | {opp.asset} {opp.event_date} | "
            f"{size:.0f} shares | {n_legs} legs | "
            f"edge={opp.edge_cents:.2f}c | cost={opp.total_cost:.4f}"
        )

        # 1. Place all buy orders concurrently
        results = []
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=n_legs) as pool:
            futures = {}
            for lg in legs:
                fut = pool.submit(
                    self._place_order,
                    lg["token"], "BUY", lg["avg_price"], size,
                )
                futures[fut] = lg

            for fut in as_completed(futures):
                lg = futures[fut]
                try:
                    result = fut.result()
                    result["description"] = lg["description"]
                    results.append(result)
                    status = "SENT" if result["success"] else "REJECTED"
                    log.info(
                        f"  {status} | {lg['description']} | "
                        f"{size:.0f} @ {lg['avg_price']:.4f} | "
                        f"order_id={result['order_id']}"
                    )
                except Exception as e:
                    log.error(f"  ERROR | {lg['description']} | {e}")
                    results.append({
                        "token_id": lg["token"],
                        "side": "BUY",
                        "price": lg["avg_price"],
                        "size": size,
                        "response": None,
                        "success": False,
                        "order_id": "",
                        "description": lg["description"],
                    })

        exec_time_ms = (time.time() - start_time) * 1000
        sent = [r for r in results if r["success"]]
        # Record orders placed for global rate limiting
        self._record_orders(len(sent))

        if not sent:
            log.warning(
                f"EXEC FAIL  | {opp.asset} {opp.event_date} | "
                f"no orders accepted"
            )
            self._log_execution(opp, size, results, "ALL_REJECTED", exec_time_ms)
            self._record_failure(pair_key)
            return False

        # 2. Wait for settlement
        time.sleep(FILL_SETTLE_DELAY_S)

        # 3. Verify fills
        confirmed = []
        failed = []
        for r in results:
            if not r["success"]:
                failed.append(r)
                continue
            fill_info = self._verify_fill(r["order_id"])
            if fill_info and fill_info["size_matched"] >= size - 0.01:
                r["verified"] = True
                r["fill_size"] = fill_info["size_matched"]
                confirmed.append(r)
                log.info(
                    f"  VERIFIED | {r['description']} | "
                    f"matched={fill_info['size_matched']:.1f}"
                )
            else:
                r["verified"] = False
                r["fill_size"] = fill_info["size_matched"] if fill_info else 0.0
                failed.append(r)
                log.warning(
                    f"  NOT FILLED | {r['description']} | "
                    f"matched={r['fill_size']:.1f} (needed {size:.0f})"
                )

        # 4. Confirm balances for verified fills
        for r in list(confirmed):  # iterate copy — safe to modify original
            balance = self._check_balance(r["token_id"])
            if balance < size - 0.01:
                log.warning(
                    f"  BALANCE LOW | {r['description']} | "
                    f"balance={balance:.1f} < needed={size:.0f}"
                )
                r["verified"] = False
                confirmed.remove(r)
                failed.append(r)

        total_exec_ms = (time.time() - start_time) * 1000

        if len(confirmed) == n_legs:
            # All legs confirmed
            total_cost = sum(r["price"] * r["size"] for r in confirmed)
            log.info(
                f"EXEC OK    | {opp.asset} {opp.event_date} | "
                f"all {n_legs} legs confirmed in {total_exec_ms:.0f}ms | "
                f"cost=${total_cost:.2f}"
            )
            self.positions[pair_key] = {
                "asset": opp.asset,
                "event_date": opp.event_date,
                "size": size,
                "entry_cost": total_cost,
                "entry_time": datetime.now(timezone.utc).isoformat(),
                "legs": confirmed,
            }
            self._save_positions()
            self._log_execution(opp, size, results, "FILLED", total_exec_ms)
            self._record_success()
            return True
        else:
            # 5. Partial fill — unwind confirmed legs
            log.warning(
                f"EXEC PARTIAL | {opp.asset} {opp.event_date} | "
                f"{len(confirmed)}/{n_legs} legs confirmed | unwinding..."
            )
            if confirmed:
                self._unwind_legs(confirmed, size)
            self._log_execution(opp, size, results, "PARTIAL_UNWOUND",
                                total_exec_ms)
            self._record_failure(pair_key)
            return False

    # ------------------------------------------------------------------
    # Unwind: sell back filled legs after partial fill
    # ------------------------------------------------------------------
    def _unwind_legs(self, legs_to_sell: list[dict], size: float):
        """Sell back legs from a partial fill.

        Fetches fresh book via REST for unwind pricing (can't rely on
        ob_manager here since the partial fill changes our urgency).
        Retries with progressively worse prices.
        """
        for r in legs_to_sell:
            sold = False
            for attempt in range(UNWIND_RETRIES):
                try:
                    # Verify we still hold shares
                    balance = self._check_balance(r["token_id"])
                    sell_size = min(size, balance)
                    if sell_size < 0.5:
                        log.info(
                            f"  UNWIND SKIP | {r['description']} | "
                            f"balance={balance:.1f} (nothing to sell)"
                        )
                        sold = True
                        break

                    # Get fresh bid from REST (most current for unwind)
                    # py-clob-client returns an OrderBookSummary object,
                    # not a dict — access .bids attribute directly.
                    book = self.client.get_order_book(r["token_id"])
                    bids = getattr(book, "bids", None) or []
                    if not bids:
                        log.warning(
                            f"  UNWIND NO BIDS | {r['description']} | "
                            f"attempt {attempt + 1}"
                        )
                        time.sleep(UNWIND_RETRY_DELAY_S)
                        continue

                    # Use best bid, drop price on retries to increase fill chance
                    # bids are OrderSummary objects with .price attribute
                    best_bid = float(bids[0].price if hasattr(bids[0], 'price') else bids[0]["price"])
                    # On retry, accept 1 tick worse per attempt
                    meta = self.meta_cache.get(r["token_id"])
                    tick = float(meta["tick_size"])
                    sell_price = max(tick, best_bid - (attempt * tick))

                    result = self._place_order(
                        r["token_id"], "SELL", sell_price, sell_size,
                    )

                    if result["success"]:
                        # Verify the sell filled
                        time.sleep(FILL_SETTLE_DELAY_S)
                        fill_info = self._verify_fill(result["order_id"])
                        if fill_info and fill_info["size_matched"] >= sell_size - 0.01:
                            log.info(
                                f"  UNWIND OK | {r['description']} | "
                                f"{sell_size:.0f} @ {sell_price:.4f}"
                            )
                            sold = True
                            break
                        else:
                            log.warning(
                                f"  UNWIND PARTIAL | {r['description']} | "
                                f"matched={fill_info['size_matched'] if fill_info else 0:.1f}"
                            )
                    else:
                        log.warning(
                            f"  UNWIND REJECTED | {r['description']} | "
                            f"attempt {attempt + 1} @ {sell_price:.4f}"
                        )

                    time.sleep(UNWIND_RETRY_DELAY_S)
                except Exception as e:
                    log.error(
                        f"  UNWIND ERROR | {r['description']} | "
                        f"attempt {attempt + 1}: {e}"
                    )
                    time.sleep(UNWIND_RETRY_DELAY_S)

            if not sold:
                log.error(
                    f"  UNWIND FAILED | {r['description']} | "
                    f"could not sell after {UNWIND_RETRIES} attempts — "
                    f"MANUAL INTERVENTION NEEDED"
                )

    # ------------------------------------------------------------------
    # Exit: sell all legs when bids sum >= $1
    # ------------------------------------------------------------------
    def check_exits(self, ob_manager):
        """Check if any open positions can be exited.

        Uses live WebSocket data from ob_manager for bid pricing.
        Only triggers the actual sell when bids sum >= $1/share.
        """
        from arb_scanner import compute_leg_sell

        to_close = []
        for pair_key, pos in self.positions.items():
            total_bid = 0.0
            can_sell = True
            bid_prices = {}  # token_id -> (avg_bid, fillable_size)

            for lg in pos["legs"]:
                avg_bid, filled = compute_leg_sell(
                    ob_manager, lg["token_id"], pos["size"]
                )
                if filled < pos["size"] - 0.01:
                    can_sell = False
                    break
                total_bid += avg_bid
                bid_prices[lg["token_id"]] = avg_bid

            if can_sell and total_bid >= 1.0:
                to_close.append((pair_key, bid_prices))

        for pair_key, bid_prices in to_close:
            self._exit_position(pair_key, bid_prices)

    def _exit_position(self, pair_key: str, bid_prices: dict[str, float]):
        """Sell all legs of a position.

        Args:
            pair_key: Position identifier.
            bid_prices: token_id -> bid price from live ob_manager data.
        """
        pos = self.positions[pair_key]
        size = pos["size"]
        n_legs = len(pos["legs"])
        log.info(
            f"EXEC SELL  | {pos['asset']} {pos['event_date']} | "
            f"selling {size:.0f} shares across {n_legs} legs"
        )

        # Verify we hold shares before selling.
        # Only sell the bot's tracked size — never touch excess shares
        # (those may be from manual trades).
        # If balance < tracked size, assume operator manually closed it.
        for lg in pos["legs"]:
            balance = self._check_balance(lg["token_id"])
            if balance < size - 0.01:
                log.info(
                    f"  SELL SKIP | {pos['asset']} {pos['event_date']} | "
                    f"{lg.get('description', '')} balance={balance:.1f} < "
                    f"tracked={size:.0f} — assuming manually closed"
                )
                del self.positions[pair_key]
                self._save_positions()
                return

        # Place all sell orders concurrently using ob_manager bid prices.
        # Sell exactly pos["size"] — leave any excess shares untouched.
        results = []
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=n_legs) as pool:
            futures = {}
            for lg in pos["legs"]:
                bid_price = bid_prices.get(lg["token_id"], 0.0)
                if bid_price <= 0:
                    continue
                fut = pool.submit(
                    self._place_order,
                    lg["token_id"], "SELL", bid_price, size,
                )
                futures[fut] = lg

            for fut in as_completed(futures):
                lg = futures[fut]
                try:
                    result = fut.result()
                    result["description"] = lg.get("description", "")
                    results.append(result)
                    status = "SENT" if result["success"] else "REJECTED"
                    log.info(
                        f"  SELL {status} | {result.get('description', '')} | "
                        f"{size:.0f} @ {result['price']:.4f} | "
                        f"order_id={result['order_id']}"
                    )
                except Exception as e:
                    log.error(f"  SELL ERROR | {lg.get('description', '')}: {e}")

        # Wait for settlement then verify
        time.sleep(FILL_SETTLE_DELAY_S)

        sell_confirmed = []
        sell_failed = []
        for r in results:
            if not r["success"]:
                sell_failed.append(r)
                continue
            fill_info = self._verify_fill(r["order_id"])
            if fill_info and fill_info["size_matched"] >= size - 0.01:
                sell_confirmed.append(r)
            else:
                sell_failed.append(r)

        exec_time_ms = (time.time() - start_time) * 1000

        if len(sell_confirmed) == n_legs:
            actual_proceeds = sum(r["price"] * r["size"] for r in sell_confirmed)
            pnl = actual_proceeds - pos["entry_cost"]
            log.info(
                f"EXEC SELL OK | {pos['asset']} {pos['event_date']} | "
                f"all {n_legs} legs sold in {exec_time_ms:.0f}ms | "
                f"proceeds=${actual_proceeds:.2f} | pnl=${pnl:.2f}"
            )
            self._log_exit(pos, sell_confirmed, pnl, exec_time_ms)
            del self.positions[pair_key]
            self._save_positions()
        elif sell_confirmed:
            # Some legs sold, some didn't — awkward state
            log.error(
                f"EXEC SELL PARTIAL | {pos['asset']} {pos['event_date']} | "
                f"{len(sell_confirmed)}/{n_legs} legs sold | "
                f"MANUAL INTERVENTION NEEDED for unsold legs"
            )
            # Don't remove position — operator needs to handle this
        else:
            log.warning(
                f"EXEC SELL FAIL | {pos['asset']} {pos['event_date']} | "
                f"no sells filled — will retry next cycle"
            )

    # ------------------------------------------------------------------
    # CSV logging
    # ------------------------------------------------------------------
    def _log_execution(self, opp, size: float, results: list[dict],
                       status: str, exec_time_ms: float):
        """Append entry execution record to CSV."""
        self._ensure_csv_header()

        filled_count = sum(1 for r in results if r.get("verified", r.get("success")))
        leg_details = " | ".join(
            f"{r.get('description', r['token_id'][:16])} "
            f"{'OK' if r.get('verified', r.get('success')) else 'FAIL'} "
            f"@ {r['price']:.4f}"
            for r in results
        )
        with open(EXECUTIONS_CSV, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now(timezone.utc).isoformat(),
                opp.asset, opp.event_date, "BUY", status, len(results),
                f"{size:.0f}", f"{opp.edge_cents:.2f}", f"{opp.total_cost:.4f}",
                f"{exec_time_ms:.0f}", filled_count, len(results),
                leg_details,
            ])

    def _log_exit(self, pos: dict, results: list[dict], pnl: float,
                  exec_time_ms: float):
        """Append exit execution record to CSV."""
        self._ensure_csv_header()

        leg_details = " | ".join(
            f"{r.get('description', r['token_id'][:16])} "
            f"@ {r['price']:.4f}"
            for r in results
        )
        proceeds = sum(r["price"] * r["size"] for r in results)
        with open(EXECUTIONS_CSV, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now(timezone.utc).isoformat(),
                pos["asset"], pos["event_date"], "SELL", "FILLED",
                len(results),
                f"{pos['size']:.0f}", "", "",
                f"{exec_time_ms:.0f}", len(results), len(results),
                f"proceeds=${proceeds:.2f} pnl=${pnl:.2f} | {leg_details}",
            ])

    def _ensure_csv_header(self):
        if not self._csv_header_written:
            with open(EXECUTIONS_CSV, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "asset", "date", "action", "status", "n_legs",
                    "shares", "edge_cents", "cost_per_share",
                    "exec_time_ms", "legs_filled", "legs_total",
                    "leg_details",
                ])
            self._csv_header_written = True

    def summary(self):
        n_open = len(self.positions)
        capital = sum(p["entry_cost"] for p in self.positions.values())
        log.info(f"EXEC SUMMARY | open={n_open} | capital=${capital:.2f}")
