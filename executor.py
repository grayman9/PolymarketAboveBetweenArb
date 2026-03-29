"""
Live order executor for arb coverings.

Places GTC limit orders for all legs of a covering.
Uses a user WebSocket for instant fill notifications.
On partial fill: keeps unfilled buy alive + posts GTC sells on filled
legs — whichever side completes first wins.
"""

import asyncio
import csv
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import websockets
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

# How long to wait for GTC limit orders to fill before evaluating
FILL_TIMEOUT_S = 180         # 3 minutes — arbs persist for minutes per duration data
FILL_POLL_INTERVAL_S = 5     # check fill status every 5s during timeout
# How long to wait after all fills before verifying balances
FILL_SETTLE_DELAY_S = 5.0
# Fill verification (REST fallback when WS is down)
FILL_VERIFY_RETRIES = 3
FILL_VERIFY_DELAY_S = 2.0

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
USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
USER_WS_PING_INTERVAL = 10  # seconds


# ---------------------------------------------------------------------------
# User WebSocket: instant fill notifications
# ---------------------------------------------------------------------------
class UserFillTracker:
    """Connects to Polymarket's user WebSocket for real-time fill events.

    Runs in a background thread. Call watch_orders() to register order IDs,
    then wait_for_fills() to block until all fill or timeout.
    """

    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self._auth = {
            "apiKey": api_key,
            "secret": api_secret,
            "passphrase": api_passphrase,
        }
        # order_id -> {"size_matched": float, "filled": bool}
        self._watched: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._fill_event = threading.Event()  # signaled on any new fill
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._running = False

    def start(self):
        """Start the background WebSocket listener thread."""
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="user-ws")
        self._thread.start()

    def stop(self):
        self._running = False
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

    def _run(self):
        """Background thread: run the async WS in its own event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        while self._running:
            try:
                self._loop.run_until_complete(self._ws_loop())
            except Exception as e:
                log.warning(f"User WS error: {e} — reconnecting in 3s...")
                time.sleep(3)

    async def _ws_loop(self):
        async with websockets.connect(USER_WS_URL,
                                      ping_interval=None) as ws:
            # Authenticate
            sub_msg = {
                "auth": self._auth,
                "type": "user",
            }
            await ws.send(json.dumps(sub_msg))
            log.info("User WS connected and authenticated")

            # After reconnect, mark that watched orders may have missed
            # fills. The wait_for_fills caller should REST-verify on timeout.
            self._ws_connected = True

            # Ping task
            async def ping():
                while self._running:
                    await asyncio.sleep(USER_WS_PING_INTERVAL)
                    try:
                        await ws.send("PING")
                    except Exception:
                        return

            ping_task = asyncio.ensure_future(ping())

            try:
                async for raw in ws:
                    if raw == "PONG":
                        continue
                    try:
                        msg = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    self._handle_message(msg)
            finally:
                ping_task.cancel()

    def _handle_message(self, msg: dict):
        """Process an incoming user WS message."""
        event_type = msg.get("event_type")

        if event_type == "trade":
            order_id = msg.get("taker_order_id", "")
            matched_size = float(msg.get("size", "0") or "0")
            with self._lock:
                if order_id in self._watched:
                    self._watched[order_id]["size_matched"] += matched_size
                    log.info(
                        f"  WS FILL | order={order_id[:16]}... | "
                        f"+{matched_size:.1f} shares | "
                        f"total={self._watched[order_id]['size_matched']:.1f}"
                    )
                    if self._watched[order_id]["size_matched"] >= \
                       self._watched[order_id]["target"] - 0.01:
                        self._watched[order_id]["filled"] = True
                    self._fill_event.set()

        elif event_type == "order":
            order_id = msg.get("id", "")
            with self._lock:
                if order_id in self._watched:
                    size_matched = float(
                        msg.get("size_matched", "0") or "0"
                    )
                    self._watched[order_id]["size_matched"] = size_matched
                    if size_matched >= \
                       self._watched[order_id]["target"] - 0.01:
                        self._watched[order_id]["filled"] = True
                        self._fill_event.set()
                    status = msg.get("status", "")
                    if status in ("CANCELED", "EXPIRED"):
                        self._watched[order_id]["cancelled"] = True
                        self._fill_event.set()

    def watch_orders(self, orders: dict[str, float]):
        """Register order IDs to watch. orders = {order_id: target_size}."""
        with self._lock:
            for oid, target in orders.items():
                self._watched[oid] = {
                    "size_matched": 0.0,
                    "target": target,
                    "filled": False,
                    "cancelled": False,
                }

    def unwatch(self, order_ids: list[str]):
        """Stop watching these order IDs."""
        with self._lock:
            for oid in order_ids:
                self._watched.pop(oid, None)

    def get_status(self, order_id: str) -> dict:
        """Get current fill status for a watched order.

        Returns empty dict if not watched — never None.
        """
        with self._lock:
            entry = self._watched.get(order_id)
            return entry.copy() if entry else {}

    def wait_for_fills(self, order_ids: list[str],
                       timeout: float) -> dict[str, dict]:
        """Block until all order_ids are filled, or timeout.

        Returns {order_id: status_dict} for all watched orders.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                all_done = all(
                    self._watched.get(oid, {}).get("filled", False) or
                    self._watched.get(oid, {}).get("cancelled", False)
                    for oid in order_ids
                )
                if all_done:
                    break
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            # Wait for any fill event, then re-check
            self._fill_event.wait(timeout=min(remaining, 1.0))
            self._fill_event.clear()

        with self._lock:
            return {oid: self._watched.get(oid, {}).copy()
                    for oid in order_ids}


# ---------------------------------------------------------------------------
class ArbExecutor:
    """Places live orders for arb coverings.

    Entry flow:
      1. Place GTC limit buys for all legs concurrently
      2. Wait for fills via user WebSocket (instant notifications)
      3. On partial fill: keep unfilled buy alive + post GTC sells on
         filled legs — whichever side completes first wins

    Exit flow:
      1. Detect exit via live WebSocket bids (ob_manager) — no REST call
      2. Verify share balances before selling
      3. Place GTC sell orders for all legs using ob_manager bids
    """

    def __init__(self, max_trade_size: float):
        self.max_trade_size = max_trade_size
        self.client: ClobClient | None = None
        self.meta_cache: TokenMetaCache | None = None
        self.fill_tracker: UserFillTracker | None = None
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

        # Start user WebSocket for instant fill notifications
        creds = self.client.creds
        self.fill_tracker = UserFillTracker(
            api_key=creds.api_key,
            api_secret=creds.api_secret,
            api_passphrase=creds.api_passphrase,
        )
        self.fill_tracker.start()

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

        For regular positions: check legs have shares, remove stale ones.
        For RACING positions: cancel any leftover orders and clean up
        based on current balances — the race can't survive a restart.
        """
        to_remove = []
        for pk, pos in self.positions.items():
            status = pos.get("status")

            if status == "RACING":
                # Cancel any leftover buy/sell orders from the race
                log.info(
                    f"  RACING | {pos['asset']} {pos['event_date']} | "
                    f"cancelling leftover orders from pre-restart race"
                )
                for order in pos.get("buy_orders", []):
                    self._cancel_order(order.get("order_id", ""))
                for order in pos.get("sell_orders", []):
                    self._cancel_order(order.get("order_id", ""))
                # Check if we still hold shares on the filled legs
                has_shares = False
                for lg in pos.get("legs", []):
                    balance = self._check_balance(lg["token_id"])
                    if balance >= 0.5:
                        has_shares = True
                        log.warning(
                            f"  RACING | {lg.get('description', '')} | "
                            f"balance={balance:.1f} — MANUAL CLEANUP NEEDED"
                        )
                if not has_shares:
                    to_remove.append(pk)
                else:
                    # Convert to a regular unhedged position for tracking
                    pos.pop("status", None)
                    pos.pop("buy_orders", None)
                    pos.pop("sell_orders", None)
                    log.warning(
                        f"  RACING → UNHEDGED | {pos['asset']} "
                        f"{pos['event_date']} | saved for manual handling"
                    )
                continue

            if status == "UNWINDING":
                # Same treatment — cancel orders, check balances
                for order in pos.get("sell_orders", []):
                    self._cancel_order(order.get("order_id", ""))
                to_remove.append(pk)
                log.info(
                    f"  UNWINDING | {pos['asset']} {pos['event_date']} | "
                    f"cleaned up — check balances manually"
                )
                continue

            # Regular position: verify leg balances
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
                     size: float,
                     order_type: OrderType = OrderType.GTC) -> dict:
        """Place an order (GTC by default). Returns result dict."""
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
        resp = self.client.post_order(signed_order, order_type)

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

    def _cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True if successful."""
        try:
            self.client.cancel(order_id)
            return True
        except Exception as e:
            log.warning(f"  Cancel failed for {order_id}: {e}")
            return False

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

        Tries CONDITIONAL first, then NEG_RISK if the token is neg_risk.
        Returns balance as float, 0.0 on error.
        """
        try:
            # Determine correct asset type from token meta cache
            meta = self.meta_cache.get(token_id)
            asset_type = "NEG_RISK" if (meta and meta.get("neg_risk")) else "CONDITIONAL"

            params = BalanceAllowanceParams(
                asset_type=asset_type,
                token_id=token_id,
            )
            result = self.client.get_balance_allowance(params=params)
            if isinstance(result, dict):
                balance = float(result.get("balance", "0") or "0")
                if balance > 0:
                    return balance

            # Fallback: try the other asset type in case meta was wrong
            fallback_type = "CONDITIONAL" if asset_type == "NEG_RISK" else "NEG_RISK"
            params = BalanceAllowanceParams(
                asset_type=fallback_type,
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
        # Polymarket taker minimums: 5 shares, $1 per order
        if size < 5:
            return False
        # Verify every leg meets $1 minimum
        for lg in opp.legs:
            if lg["avg_price"] * size < 1.0:
                log.debug(
                    f"EXEC SKIP | {lg['description']} | "
                    f"${lg['avg_price'] * size:.2f} < $1 minimum"
                )
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
        """Inner execution logic, wrapped by execute_opportunity's safety net.

        Flow:
          1. Place GTC limit buys for all legs concurrently
          2. Wait for fills via user WebSocket (instant notifications)
          3. After timeout, evaluate:
             - All legs filled → position confirmed
             - Partial → keep unfilled buys alive + post GTC sells on
               filled legs. Whichever side completes first wins.
        """
        legs = opp.legs
        log.info(
            f"EXEC BUY   | {opp.asset} {opp.event_date} | "
            f"{size:.0f} shares | {n_legs} legs | "
            f"edge={opp.edge_cents:.2f}c | cost={opp.total_cost:.4f}"
        )

        # 1. Place all GTC limit buy orders concurrently
        results = []
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=n_legs) as pool:
            futures = {}
            for lg in legs:
                fut = pool.submit(
                    self._place_order,
                    lg["token"], "BUY", lg["avg_price"], size,
                    OrderType.GTC,
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

        sent = [r for r in results if r["success"]]
        self._record_orders(len(sent))

        if not sent:
            log.warning(
                f"EXEC FAIL  | {opp.asset} {opp.event_date} | "
                f"no orders accepted"
            )
            exec_time_ms = (time.time() - start_time) * 1000
            self._log_execution(opp, size, results, "ALL_REJECTED", exec_time_ms)
            self._record_failure(pair_key)
            return False

        # 2. Register orders with the fill tracker and wait
        watch_map = {r["order_id"]: size for r in sent}
        self.fill_tracker.watch_orders(watch_map)

        log.info(
            f"  WAITING  | {len(sent)} GTC orders placed, "
            f"waiting up to {FILL_TIMEOUT_S}s via user WS..."
        )

        fill_status = self.fill_tracker.wait_for_fills(
            list(watch_map.keys()), timeout=FILL_TIMEOUT_S
        )

        # 3. Evaluate results — use WS status, then REST-verify unfilled
        confirmed = []
        unfilled = []
        for r in sent:
            st = fill_status.get(r["order_id"], {})
            if st.get("filled"):
                r["verified"] = True
                r["fill_size"] = st.get("size_matched", 0.0)
                confirmed.append(r)
                log.info(
                    f"  FILLED   | {r['description']} | "
                    f"matched={r['fill_size']:.1f}"
                )
            else:
                # REST fallback: WS may have missed fills during reconnect
                fill_info = self._verify_fill(r["order_id"])
                if fill_info and fill_info["size_matched"] >= size - 0.01:
                    r["verified"] = True
                    r["fill_size"] = fill_info["size_matched"]
                    confirmed.append(r)
                    log.info(
                        f"  FILLED (REST) | {r['description']} | "
                        f"matched={fill_info['size_matched']:.1f}"
                    )
                else:
                    r["verified"] = False
                    r["fill_size"] = st.get("size_matched", 0.0)
                    unfilled.append(r)
                    log.warning(
                        f"  UNFILLED | {r['description']} | "
                        f"matched={r['fill_size']:.1f} (needed {size:.0f})"
                    )

        self.fill_tracker.unwatch(list(watch_map.keys()))
        total_exec_ms = (time.time() - start_time) * 1000

        if len(confirmed) == n_legs:
            # All legs filled — position confirmed
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
        elif not confirmed:
            # Nothing filled — cancel all and move on
            for r in unfilled:
                self._cancel_order(r["order_id"])
                log.info(f"  CANCELLED | {r['description']}")
            self._log_execution(opp, size, results, "NONE_FILLED",
                                total_exec_ms)
            self._record_failure(pair_key)
            return False
        else:
            # Partial fill — smart unwind:
            #   Keep unfilled buys alive (they might still fill)
            #   Post GTC sells on filled legs at entry price
            #   Monitor both sides — whichever completes first wins
            log.warning(
                f"EXEC PARTIAL | {opp.asset} {opp.event_date} | "
                f"{len(confirmed)}/{n_legs} legs filled | "
                f"racing: keeping buys + posting sells..."
            )
            self._race_unwind(
                confirmed, unfilled, size, opp, pair_key
            )
            self._log_execution(opp, size, results, "PARTIAL_RACING",
                                total_exec_ms)
            self._record_failure(pair_key)
            return False

    # ------------------------------------------------------------------
    # Smart unwind: race buys vs sells after partial fill
    # ------------------------------------------------------------------
    def _race_unwind(self, filled_legs: list[dict],
                     unfilled_legs: list[dict], size: float,
                     opp, pair_key: str):
        """Handle partial fill by racing both sides.

        Strategy:
          - Keep unfilled buy GTC orders alive (they might still fill)
          - Post GTC sell orders at entry price on filled legs
          - Monitor via user WS — whichever side completes first:
            - All buys fill → cancel sells, position is complete
            - All sells fill → cancel remaining buys, clean exit
          - Save to positions for tracking either way
        """
        # Wait for settlement so filled shares are available to sell
        log.info(f"  RACE | waiting {FILL_SETTLE_DELAY_S}s for settlement...")
        time.sleep(FILL_SETTLE_DELAY_S)

        # Post GTC sells on filled legs at entry price.
        # Use entry price (not current bid) — this is a passive order
        # that sits on the book. We're not trying to force an immediate
        # fill, we're racing against the remaining buys filling.
        # Entry price is where we bought, so selling at entry = breakeven.
        sell_results = []
        for r in filled_legs:
            sell_price = r["price"]
            try:
                result = self._place_order(
                    r["token_id"], "SELL", sell_price, size,
                    OrderType.GTC,
                )
                if result["success"]:
                    log.info(
                        f"  RACE SELL | {r['description']} | "
                        f"{size:.0f} @ {sell_price:.4f} | "
                        f"order_id={result['order_id']}"
                    )
                    sell_results.append({
                        "order_id": result["order_id"],
                        "token_id": r["token_id"],
                        "description": r["description"],
                        "price": r["price"],
                        "size": size,
                    })
                else:
                    log.warning(
                        f"  RACE SELL REJECTED | {r['description']}"
                    )
            except Exception as e:
                log.error(f"  RACE SELL ERROR | {r['description']} | {e}")

        # Register all orders (remaining buys + new sells) with fill tracker
        buy_ids = {r["order_id"]: size for r in unfilled_legs}
        sell_ids = {s["order_id"]: size for s in sell_results}
        all_race_ids = {**buy_ids, **sell_ids}
        self.fill_tracker.watch_orders(all_race_ids)

        # Save position so it's tracked across restarts
        race_key = f"{pair_key}|race"
        self.positions[race_key] = {
            "asset": opp.asset,
            "event_date": opp.event_date,
            "size": size,
            "entry_cost": sum(r["price"] * size for r in filled_legs),
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "legs": filled_legs,
            "status": "RACING",
            "buy_orders": [{"order_id": r["order_id"],
                            "token_id": r["token_id"],
                            "description": r["description"],
                            "price": r["price"],
                            "size": size} for r in unfilled_legs],
            "sell_orders": sell_results,
        }
        self._save_positions()

        log.info(
            f"  RACE | {len(buy_ids)} buys + {len(sell_ids)} sells active | "
            f"waiting up to {FILL_TIMEOUT_S}s..."
        )

        # Monitor: check periodically which side is winning
        deadline = time.time() + FILL_TIMEOUT_S
        outcome = None

        while time.time() < deadline and outcome is None:
            self.fill_tracker.wait_for_fills(
                list(all_race_ids.keys()),
                timeout=min(FILL_POLL_INTERVAL_S, deadline - time.time()),
            )

            # Check if all buys filled
            buys_done = all(
                self.fill_tracker.get_status(oid).get("filled", False)
                for oid in buy_ids
            )
            # Check if all sells filled
            sells_done = all(
                self.fill_tracker.get_status(oid).get("filled", False)
                for oid in sell_ids
            ) if sell_ids else False

            if buys_done:
                outcome = "BUYS_WON"
            elif sells_done:
                outcome = "SELLS_WON"

        self.fill_tracker.unwatch(list(all_race_ids.keys()))

        if outcome == "BUYS_WON":
            # All buys filled — cancel sells, position is complete!
            log.info(
                f"  RACE WON | all buys filled — cancelling sells, "
                f"position complete!"
            )
            for s in sell_results:
                self._cancel_order(s["order_id"])

            # Update unfilled legs with actual fill data before merging
            for r in unfilled_legs:
                fill_info = self._verify_fill(r["order_id"])
                if fill_info:
                    r["verified"] = True
                    r["fill_size"] = fill_info["size_matched"]
            all_legs = filled_legs + unfilled_legs
            total_cost = sum(r["price"] * size for r in all_legs)
            del self.positions[race_key]
            self.positions[pair_key] = {
                "asset": opp.asset,
                "event_date": opp.event_date,
                "size": size,
                "entry_cost": total_cost,
                "entry_time": datetime.now(timezone.utc).isoformat(),
                "legs": all_legs,
            }
            self._save_positions()
            self._record_success()

        elif outcome == "SELLS_WON":
            # All sells filled — cancel remaining buys, clean exit
            log.info(
                f"  RACE EXIT | all sells filled — cancelling buys, "
                f"clean exit"
            )
            for oid in buy_ids:
                self._cancel_order(oid)
            del self.positions[race_key]
            self._save_positions()

        else:
            # Timeout — leave everything as-is for manual handling
            log.warning(
                f"  RACE TIMEOUT | neither side completed in "
                f"{FILL_TIMEOUT_S}s — position saved as '{race_key}' "
                f"for manual handling"
            )

    # ------------------------------------------------------------------
    # Exit: sell all legs when bids sum >= $1
    # ------------------------------------------------------------------
    def check_exits(self, ob_manager):
        """Check if any open positions can be exited.

        Uses live WebSocket data from ob_manager for bid pricing.
        Only triggers the actual sell when bids sum >= $1/share.
        Skips positions with status RACING/UNWINDING (handled separately).
        """
        from arb_scanner import compute_leg_sell

        to_close = []
        for pair_key, pos in self.positions.items():
            # Skip race/unwind positions — they have their own logic
            if pos.get("status") in ("RACING", "UNWINDING"):
                continue

            total_bid = 0.0
            can_sell = True
            bid_prices = {}

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
        """Sell all legs of a position using GTC limit orders.

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

        # Place GTC sell orders concurrently at ob_manager bid prices
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
                    OrderType.GTC,
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

        sent = [r for r in results if r["success"]]
        if not sent:
            log.warning(
                f"EXEC SELL FAIL | {pos['asset']} {pos['event_date']} | "
                f"no sells accepted — will retry next cycle"
            )
            return

        # Wait for fills via user WS
        watch_map = {r["order_id"]: size for r in sent}
        self.fill_tracker.watch_orders(watch_map)
        fill_status = self.fill_tracker.wait_for_fills(
            list(watch_map.keys()), timeout=FILL_TIMEOUT_S
        )
        self.fill_tracker.unwatch(list(watch_map.keys()))

        sell_confirmed = [
            r for r in sent
            if fill_status.get(r["order_id"], {}).get("filled")
        ]
        sell_unfilled = [
            r for r in sent
            if not fill_status.get(r["order_id"], {}).get("filled")
        ]

        # Cancel unfilled sells
        for r in sell_unfilled:
            self._cancel_order(r["order_id"])

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
            # Update position to only contain unsold legs
            sold_tokens = {r["token_id"] for r in sell_confirmed}
            remaining_legs = [
                lg for lg in pos["legs"]
                if lg["token_id"] not in sold_tokens
            ]
            sold_proceeds = sum(r["price"] * r["size"] for r in sell_confirmed)
            pos["legs"] = remaining_legs
            pos["entry_cost"] = max(0, pos["entry_cost"] - sold_proceeds)
            self._save_positions()
            log.warning(
                f"EXEC SELL PARTIAL | {pos['asset']} {pos['event_date']} | "
                f"{len(sell_confirmed)}/{n_legs} legs sold | "
                f"{len(remaining_legs)} legs remaining — will retry next cycle"
            )
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
