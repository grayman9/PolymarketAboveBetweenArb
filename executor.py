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

# How long to wait for initial GTC fills before adaptive pricing kicks in
INITIAL_FILL_WAIT_S = 15     # 15s — if a leg hasn't filled, start bumping
# How long between each price bump on unfilled legs
ADAPTIVE_TICK_INTERVAL_S = 10  # bump by 1 tick every 10s
# Max total time for adaptive pricing before falling back to race
ADAPTIVE_MAX_TIME_S = 120     # 2 minutes of adaptive pricing
# Grace period before posting sells on a racing position
RACE_SELL_GRACE_S = 600      # 10 minutes — give the buy time to fill first
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
        # Background execution thread
        self._exec_thread: threading.Thread | None = None
        # Lock for positions dict (executor thread + main loop both access)
        self._positions_lock = threading.Lock()

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

        self._proxy_address = proxy_address
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
        """Write open positions to disk as JSON (thread-safe)."""
        try:
            with self._positions_lock:
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
        for pk, pos in list(self.positions.items()):
            status = pos.get("status")

            if status == "PENDING":
                # Crashed mid-execution. Check which tokens have balances.
                log.info(
                    f"  PENDING | {pos['asset']} {pos['event_date']} | "
                    f"crashed mid-execution — checking balances"
                )
                token_ids = pos.get("token_ids", [])
                prices = pos.get("prices", {})
                has_shares = False
                legs_with_shares = []
                for tid in token_ids:
                    balance = self._check_balance(tid)
                    if balance >= 0.5:
                        has_shares = True
                        legs_with_shares.append({
                            "token_id": tid,
                            "price": prices.get(tid, 0),
                            "size": balance,
                            "description": f"token {tid[:16]}...",
                        })
                        log.warning(
                            f"  PENDING | {tid[:16]}... | "
                            f"balance={balance:.1f} — orphaned shares!"
                        )
                    # Cancel any lingering orders
                    try:
                        self.client.cancel_market_orders(asset_id=tid)
                    except Exception:
                        pass

                if has_shares:
                    # Convert to RACING so check_races() handles it
                    # Find which tokens DON'T have shares — those are the buy side
                    buy_tokens = [tid for tid in token_ids
                                  if tid not in {l["token_id"] for l in legs_with_shares}]
                    buy_orders = []
                    for tid in buy_tokens:
                        price = prices.get(tid, 0)
                        if price > 0:
                            result = self._place_order(
                                tid, "BUY", price, legs_with_shares[0]["size"],
                                OrderType.GTC,
                            )
                            if result["success"]:
                                buy_orders.append({
                                    "order_id": result["order_id"],
                                    "token_id": tid,
                                    "description": f"token {tid[:16]}...",
                                    "price": price,
                                    "size": legs_with_shares[0]["size"],
                                })
                    if buy_orders:
                        race_key = pk.replace("|pending", "|race")
                        self.positions[race_key] = {
                            "asset": pos["asset"],
                            "event_date": pos["event_date"],
                            "size": legs_with_shares[0]["size"],
                            "entry_cost": sum(l["price"] * l["size"] for l in legs_with_shares),
                            "entry_time": pos.get("entry_time", datetime.now(timezone.utc).isoformat()),
                            "race_started": time.time(),
                            "legs": legs_with_shares,
                            "status": "RACING",
                            "buy_orders": buy_orders,
                            "sell_orders": [],
                            "sells_posted": False,
                        }
                        log.info(
                            f"  PENDING → RACING | {len(legs_with_shares)} legs held, "
                            f"{len(buy_orders)} buys placed"
                        )
                    else:
                        log.warning(
                            f"  PENDING | orphaned shares but no buy tokens — "
                            f"MANUAL CLEANUP NEEDED"
                        )
                to_remove.append(pk)
                continue

            if status == "RACING":
                # Resume the race: check which orders are still live,
                # re-place sells if needed, and let check_races() monitor.
                log.info(
                    f"  RACING | {pos['asset']} {pos['event_date']} | "
                    f"resuming race from previous session"
                )
                # Check if buy orders are still live
                buy_orders = pos.get("buy_orders", [])
                sell_orders = pos.get("sell_orders", [])

                # Verify buys — check if they filled while we were down
                for bo in buy_orders:
                    fill_info = self._verify_fill(bo.get("order_id", ""))
                    if fill_info and fill_info["size_matched"] >= bo["size"] - 0.01:
                        log.info(
                            f"  RACING | buy filled while offline: "
                            f"{bo.get('description', '')}"
                        )
                        bo["filled"] = True

                all_buys_filled = all(bo.get("filled") for bo in buy_orders)
                if all_buys_filled:
                    # All buys filled — cancel sells, convert to full position
                    log.info(f"  RACING → COMPLETE | all buys filled!")
                    for so in sell_orders:
                        self._cancel_order(so.get("order_id", ""))
                    pos.pop("status", None)
                    pos.pop("buy_orders", None)
                    pos.pop("sell_orders", None)
                    continue

                # Check if we still hold shares on filled legs
                has_shares = False
                for lg in pos.get("legs", []):
                    balance = self._check_balance(lg["token_id"])
                    if balance >= 0.5:
                        has_shares = True
                if not has_shares:
                    # All sells filled while offline — cancel buys
                    log.info(f"  RACING → EXITED | sells filled, cancelling buys")
                    for bo in buy_orders:
                        self._cancel_order(bo.get("order_id", ""))
                    to_remove.append(pk)
                    continue

                # Race still active — cancel ALL orders on all tokens
                # (filled legs + buy legs — previous session may have left
                # orphaned orders we don't know about).
                all_token_ids = set()
                for lg in pos.get("legs", []):
                    all_token_ids.add(lg["token_id"])
                for bo in buy_orders:
                    all_token_ids.add(bo.get("token_id", ""))
                for tid in all_token_ids:
                    if not tid:
                        continue
                    try:
                        self.client.cancel_market_orders(asset_id=tid)
                        log.info(
                            f"  RACING | cancelled all orders on "
                            f"{tid[:16]}..."
                        )
                    except Exception as e:
                        log.warning(f"  Cancel market orders failed: {e}")

                # Check actual balances on ALL tokens (filled + buy legs)
                # to determine the real state after restart.
                buy_token_balances = {}
                for bo in buy_orders:
                    tid = bo.get("token_id", "")
                    if tid:
                        buy_token_balances[tid] = self._check_balance(tid)

                filled_leg_balances = {}
                for lg in pos.get("legs", []):
                    filled_leg_balances[lg["token_id"]] = self._check_balance(
                        lg["token_id"]
                    )

                # If the buy leg now has shares, it filled (partially or fully)
                # while we were offline. Update the position accordingly.
                total_buy_filled = sum(buy_token_balances.values())
                target = pos["size"]

                if total_buy_filled >= target - 0.01:
                    # Buy fully filled while offline — complete position!
                    log.info(
                        f"  RACING → COMPLETE | buy filled while offline "
                        f"({total_buy_filled:.1f}/{target:.0f})"
                    )
                    base_key = pk.replace("|race", "")
                    pos.pop("status", None)
                    pos.pop("buy_orders", None)
                    pos.pop("sell_orders", None)
                    pos.pop("sells_posted", None)
                    pos.pop("race_started", None)
                    # Add buy legs to the position
                    for bo in buy_orders:
                        tid = bo.get("token_id", "")
                        bal = buy_token_balances.get(tid, 0)
                        if bal >= 0.5:
                            pos["legs"].append({
                                "token_id": tid,
                                "description": bo.get("description", ""),
                                "price": bo["price"],
                                "size": bal,
                            })
                    self.positions[base_key] = pos
                    if pk != base_key:
                        del self.positions[pk]
                    self._save_positions()
                    continue

                if sum(filled_leg_balances.values()) < 0.5:
                    # All filled legs sold while offline — clean exit
                    log.info(f"  RACING → EXITED | no balance on filled legs")
                    to_remove.append(pk)
                    continue

                # Partial state — re-place buy at aggressive price,
                # let check_races() handle sells after grace period.
                # Size the new buy to match what we actually hold.
                actual_hold = min(filled_leg_balances.values()) if filled_leg_balances else 0
                new_buy_size = actual_hold  # buy enough to cover what we hold

                new_buy_orders = []
                for bo in buy_orders:
                    if new_buy_size < 0.5:
                        break
                    # Re-place buy at same price
                    result = self._place_order(
                        bo["token_id"], "BUY", bo["price"], new_buy_size,
                        OrderType.GTC,
                    )
                    if result["success"]:
                        new_buy_orders.append({
                            "order_id": result["order_id"],
                            "token_id": bo["token_id"],
                            "description": bo.get("description", ""),
                            "price": bo["price"],
                            "size": new_buy_size,
                        })
                        log.info(
                            f"  RACING | re-placed buy: "
                            f"{bo.get('description', '')} | "
                            f"{new_buy_size:.2f} @ {bo['price']:.4f}"
                        )

                pos["buy_orders"] = new_buy_orders
                pos["sell_orders"] = []
                pos["sells_posted"] = False
                pos["size"] = new_buy_size
                pos["race_started"] = time.time()
                self._save_positions()
                log.info(
                    f"  RACING | {pos['asset']} {pos['event_date']} | "
                    f"{len(buy_orders)} buys + {len(sell_orders)} sells active"
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

            # Regular position: verify leg balances via data API
            all_legs_ok = True
            api_failed = False
            for lg in pos["legs"]:
                balance = self._check_balance(lg["token_id"])
                if balance < 0:
                    # API failed — don't make decisions on bad data
                    api_failed = True
                    break
                if balance < pos["size"] - 0.01:
                    all_legs_ok = False
                    log.info(
                        f"  STALE | {pos['asset']} {pos['event_date']} | "
                        f"{lg.get('description', lg['token_id'][:16])} "
                        f"balance={balance:.1f} < tracked={pos['size']:.0f} — "
                        f"assuming manually closed"
                    )
                    break

            if api_failed:
                log.warning(
                    f"  API DOWN | {pos['asset']} {pos['event_date']} | "
                    f"keeping position — can't verify balances"
                )
            elif all_legs_ok:
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

        Uses the data-api positions endpoint which returns actual on-chain
        holdings for the proxy wallet. Returns -1.0 on API failure (so
        callers can distinguish "no shares" from "API down").
        """
        try:
            import requests as _req
            resp = _req.get(
                "https://data-api.polymarket.com/positions",
                params={
                    "user": self._proxy_address,
                    "sizeThreshold": "0",
                },
                timeout=10,
            )
            if resp.status_code != 200:
                log.warning(f"  Balance check HTTP {resp.status_code}")
                return -1.0  # API error, not "0 balance"
            for pos in resp.json():
                if pos.get("asset") == token_id:
                    return float(pos.get("size", 0))
            return 0.0  # API succeeded, token not found = 0 balance
        except Exception as e:
            log.warning(f"  Balance check failed for {token_id[:16]}...: {e}")
            return -1.0  # API error

    # ------------------------------------------------------------------
    # Entry: buy all legs
    # ------------------------------------------------------------------
    def is_executing(self) -> bool:
        """Check if an execution is currently running in the background."""
        return self._exec_thread is not None and self._exec_thread.is_alive()

    def execute_in_background(self, opp, pair_key: str, ob_manager=None):
        """Run execute_opportunity in a background thread.

        The WS loop continues processing messages while execution runs.
        Only one execution at a time — is_executing() gates new entries.
        """
        if self.is_executing():
            return
        self._exec_thread = threading.Thread(
            target=self.execute_opportunity,
            args=(opp, pair_key, ob_manager),
            daemon=True,
            name="executor",
        )
        self._exec_thread.start()

    def execute_opportunity(self, opp, pair_key: str, ob_manager=None) -> bool:
        """Execute an arb opportunity.

        Steps:
          0. Pre-execute safety checks
          1. Dynamic size-down to thinnest leg's depth
          2. Pre-flight depth check on all legs
          3. Compute aggressive prices (thin legs get 1-2 ticks above ask)
          4. FAK sweep on all legs (instant partial fills)
          5. If any leg got zero → immediate unwind
          6. GTC for remainder + adaptive pricing
          7. If still partial after adaptive → race (keep buys + post sells)

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

        try:
            result = self._execute_inner(opp, pair_key, size, n_legs, ob_manager)
        except Exception as e:
            log.error(
                f"EXEC CRASH | {opp.asset} {opp.event_date} | "
                f"{type(e).__name__}: {e}"
            )
            self._record_failure(pair_key)
            result = False
        finally:
            # Clean up PENDING marker — execution completed (success or fail).
            # If a RACING position was created, it replaces PENDING.
            pending_key = f"{pair_key}|pending"
            self.positions.pop(pending_key, None)
            self._save_positions()
        return result

    # ------------------------------------------------------------------
    # Pre-flight checks and pricing
    # ------------------------------------------------------------------
    def _get_ask_depth(self, token_id: str, ob_manager) -> tuple[float, float]:
        """Return (best_ask_price, total_ask_depth) from live orderbook."""
        if ob_manager is None:
            return (0.0, 0.0)
        book = ob_manager.books.get(token_id)
        if not book or not book.asks:
            return (0.0, 0.0)
        best_ask = book.asks[0].price
        total_depth = sum(lvl.size for lvl in book.asks)
        return (best_ask, total_depth)

    def _dynamic_size_down(self, legs: list[dict], target_size: float,
                           ob_manager) -> float:
        """Cap trade size to the thinnest leg's available depth."""
        if ob_manager is None:
            return target_size
        min_depth = float('inf')
        thin_desc = ""
        for lg in legs:
            _, depth = self._get_ask_depth(lg["token"], ob_manager)
            if depth < min_depth:
                min_depth = depth
                thin_desc = lg["description"]
        if min_depth < target_size:
            new_size = max(int(min_depth), 5)  # floor to int, enforce minimum
            if new_size < 5:
                return 0  # can't trade
            log.info(
                f"  SIZE DOWN | {thin_desc} depth={min_depth:.1f} | "
                f"capping {target_size:.0f} → {new_size}"
            )
            return new_size
        return target_size

    def _preflight_check(self, legs: list[dict], size: float,
                         ob_manager) -> bool:
        """Verify all legs still have asks >= size. Returns False to abort."""
        if ob_manager is None:
            return True  # no ob_manager = skip check
        for lg in legs:
            _, depth = self._get_ask_depth(lg["token"], ob_manager)
            if depth < size - 0.01:
                log.warning(
                    f"  PREFLIGHT FAIL | {lg['description']} | "
                    f"ask_depth={depth:.1f} < target={size:.0f}"
                )
                return False
        return True

    def _compute_leg_prices(self, legs: list[dict], size: float,
                            ob_manager) -> list[dict]:
        """Compute order prices per leg. Thin legs get 1-2 ticks above ask."""
        enriched = []
        depths = []
        for lg in legs:
            best_ask, depth = self._get_ask_depth(lg["token"], ob_manager)
            if best_ask == 0:
                best_ask = lg["avg_price"]
            depths.append(depth)
            enriched.append({
                "token": lg["token"],
                "description": lg["description"],
                "side": lg["side"],
                "avg_price": lg["avg_price"],
                "best_ask": best_ask,
                "depth": depth,
                "leg_cost_usd": lg.get("leg_cost_usd", 0),
            })

        min_depth = min(depths) if depths else 0
        total_price = 0.0

        for e in enriched:
            meta = self.meta_cache.get(e["token"])
            tick = float(meta["tick_size"])

            if e["depth"] <= min_depth + 0.01:
                # Thin leg: 2 ticks above if very thin, 1 tick otherwise
                ticks_above = 2 if e["depth"] < size * 0.5 else 1
                e["order_price"] = e["best_ask"] + tick * ticks_above
                e["is_thin"] = True
            else:
                e["order_price"] = e["best_ask"]
                e["is_thin"] = False
            total_price += e["order_price"]

        # If aggressive pricing pushes total over breakeven, back off
        while total_price > 1.0:
            # Find the leg with the highest order_price above best_ask
            max_bump_leg = None
            max_bump = 0
            for e in enriched:
                bump = e["order_price"] - e["best_ask"]
                if bump > max_bump:
                    max_bump = bump
                    max_bump_leg = e
            if max_bump_leg is None or max_bump < 0.001:
                break
            meta = self.meta_cache.get(max_bump_leg["token"])
            tick = float(meta["tick_size"])
            max_bump_leg["order_price"] -= tick
            total_price -= tick

        return enriched

    # ------------------------------------------------------------------
    # Entry: FAK sweep + GTC remainder + adaptive pricing
    # ------------------------------------------------------------------
    def _execute_inner(self, opp, pair_key: str, size: float,
                       n_legs: int, ob_manager=None) -> bool:
        """Execute with FAK-first strategy and adaptive pricing.

        Flow:
          1. Dynamic size-down to thinnest leg
          2. Pre-flight depth check
          3. Compute aggressive prices (thin legs get tick boost)
          4. FAK sweep on all legs (instant partial fills)
          5. If any leg got zero → immediate unwind
          6. GTC for remainder + adaptive pricing on unfilled shares
          7. If still partial after adaptive → race
        """
        legs = opp.legs
        max_total_cost = 1.0
        start_time = time.time()

        # 1. Dynamic size-down
        if ob_manager:
            size = self._dynamic_size_down(legs, size, ob_manager)
            if size < 5:
                return False
            # Re-check $1 minimum per leg at new size
            for lg in legs:
                if lg["avg_price"] * size < 1.0:
                    return False

        # 2. Pre-flight depth check
        if not self._preflight_check(legs, size, ob_manager):
            return False

        # 3. Compute prices (thin legs get boosted)
        if ob_manager:
            enriched = self._compute_leg_prices(legs, size, ob_manager)
        else:
            enriched = [{
                "token": lg["token"], "description": lg["description"],
                "order_price": lg["avg_price"], "best_ask": lg["avg_price"],
                "is_thin": False, "depth": 999, "side": lg["side"],
                "avg_price": lg["avg_price"],
            } for lg in legs]

        log.info(
            f"EXEC BUY   | {opp.asset} {opp.event_date} | "
            f"{size:.0f} shares | {n_legs} legs | "
            f"edge={opp.edge_cents:.2f}c | cost={opp.total_cost:.4f}"
        )
        for e in enriched:
            tag = " (THIN +tick)" if e["is_thin"] else ""
            # Log the orderbook levels we're executing against
            book_str = ""
            if ob_manager:
                book = ob_manager.books.get(e["token"])
                if book and book.asks:
                    levels = [f"{lvl.size:.1f}@{lvl.price:.4f}"
                              for lvl in book.asks[:5]]
                    book_str = f" | book=[{', '.join(levels)}]"
            log.info(
                f"  PRICE | {e['description']} | "
                f"ask={e['best_ask']:.4f} → order={e['order_price']:.4f} | "
                f"depth={e['depth']:.0f}{tag}{book_str}"
            )

        # Save PENDING position before placing any orders.
        # If we crash mid-execution, reconciliation will see this and
        # check balances to figure out what actually happened.
        pending_key = f"{pair_key}|pending"
        self.positions[pending_key] = {
            "asset": opp.asset,
            "event_date": opp.event_date,
            "size": size,
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "status": "PENDING",
            "token_ids": [e["token"] for e in enriched],
            "prices": {e["token"]: e["order_price"] for e in enriched},
        }
        self._save_positions()

        # 4. FAK sweep — instant partial fills
        fak_results = []
        with ThreadPoolExecutor(max_workers=n_legs) as pool:
            futures = {}
            for e in enriched:
                fut = pool.submit(
                    self._place_order,
                    e["token"], "BUY", e["order_price"], size,
                    OrderType.FAK,
                )
                futures[fut] = e

            for fut in as_completed(futures):
                e = futures[fut]
                try:
                    result = fut.result()
                    result["description"] = e["description"]
                    result["current_price"] = e["order_price"]
                    result["is_thin"] = e["is_thin"]
                    fak_results.append(result)
                    status = "FAK OK" if result["success"] else "FAK FAIL"
                    log.info(
                        f"  {status} | {e['description']} | "
                        f"{size:.0f} @ {e['order_price']:.4f} | "
                        f"order_id={result['order_id']}"
                    )
                except Exception as ex:
                    log.error(f"  FAK ERROR | {e['description']} | {ex}")
                    fak_results.append({
                        "token_id": e["token"], "side": "BUY",
                        "price": e["order_price"],
                        "current_price": e["order_price"],
                        "size": size, "response": None,
                        "success": False, "order_id": "",
                        "description": e["description"],
                        "is_thin": e["is_thin"],
                    })

        self._record_orders(sum(1 for r in fak_results if r["success"]))

        # Verify FAK fills via REST (FAK is instant, results available immediately)
        time.sleep(0.5)  # brief settle
        for r in fak_results:
            if r["success"] and r["order_id"]:
                fill_info = self._verify_fill(r["order_id"])
                r["fak_filled"] = fill_info["size_matched"] if fill_info else 0.0
            else:
                r["fak_filled"] = 0.0
            log.info(
                f"  FAK FILL | {r['description']} | "
                f"filled={r['fak_filled']:.1f}/{size:.0f}"
            )

        # 5. Evaluate FAK results
        min_filled = min(r["fak_filled"] for r in fak_results)
        any_zero = any(r["fak_filled"] < 0.01 for r in fak_results)
        all_complete = all(r["fak_filled"] >= size - 0.01 for r in fak_results)

        if all_complete:
            # All legs fully filled by FAK — done!
            total_cost = sum(r["current_price"] * size for r in fak_results)
            exec_time_ms = (time.time() - start_time) * 1000
            log.info(
                f"EXEC OK    | {opp.asset} {opp.event_date} | "
                f"all {n_legs} legs FAK-filled in {exec_time_ms:.0f}ms | "
                f"cost=${total_cost:.2f}"
            )
            self.positions[pair_key] = {
                "asset": opp.asset,
                "event_date": opp.event_date,
                "size": size,
                "entry_cost": total_cost,
                "entry_time": datetime.now(timezone.utc).isoformat(),
                "legs": [{
                    "token_id": r["token_id"],
                    "description": r["description"],
                    "price": r["current_price"],
                    "size": r["fak_filled"],
                    "verified": True,
                    "fill_size": r["fak_filled"],
                } for r in fak_results],
            }
            self._save_positions()
            self._log_execution(opp, size, fak_results, "FILLED",
                                exec_time_ms)
            self._record_success()
            return True

        if any_zero:
            # At least one leg got nothing — immediate unwind
            log.warning(
                f"EXEC ZERO LEG | {opp.asset} {opp.event_date} | "
                f"one leg got 0 fills — unwinding..."
            )
            filled_legs = [r for r in fak_results if r["fak_filled"] >= 0.01]
            unfilled_legs = [r for r in fak_results if r["fak_filled"] < 0.01]

            if filled_legs:
                # Race: post sells on filled legs, keep trying to buy the empty leg
                # Place GTC buys on the zero-fill legs at aggressive price
                for r in unfilled_legs:
                    meta = self.meta_cache.get(r["token_id"])
                    tick = float(meta["tick_size"])
                    aggressive_price = r["current_price"] + tick * 2
                    # Budget check
                    other_cost = sum(
                        fr["current_price"] for fr in fak_results
                        if fr["token_id"] != r["token_id"]
                    )
                    if aggressive_price + other_cost > max_total_cost:
                        aggressive_price = r["current_price"]

                    result = self._place_order(
                        r["token_id"], "BUY", aggressive_price, size,
                        OrderType.GTC,
                    )
                    if result["success"]:
                        r["order_id"] = result["order_id"]
                        r["current_price"] = aggressive_price
                        r["price"] = aggressive_price
                        log.info(
                            f"  GTC BUY (aggressive) | {r['description']} | "
                            f"{size:.0f} @ {aggressive_price:.4f}"
                        )

                # Convert filled FAK results to the format _race_unwind expects
                confirmed = [{
                    "token_id": r["token_id"],
                    "description": r["description"],
                    "price": r["current_price"],
                    "size": size,
                    "order_id": r["order_id"],
                    "verified": True,
                    "fill_size": r["fak_filled"],
                } for r in filled_legs]

                unfilled_for_race = [{
                    "token_id": r["token_id"],
                    "description": r["description"],
                    "price": r["current_price"],
                    "size": size,
                    "order_id": r["order_id"],
                } for r in unfilled_legs if r.get("order_id")]

                if unfilled_for_race:
                    self._race_unwind(
                        confirmed, unfilled_for_race, size, opp, pair_key
                    )
                    exec_time_ms = (time.time() - start_time) * 1000
                    self._log_execution(opp, size, fak_results,
                                        "FAK_ZERO_RACING", exec_time_ms)
                    self._record_failure(pair_key)
                    return False

            exec_time_ms = (time.time() - start_time) * 1000
            self._log_execution(opp, size, fak_results, "FAK_ZERO_ABORT",
                                exec_time_ms)
            self._record_failure(pair_key)
            return False

        # 6. All legs got partial fills — place GTC for remainder
        log.info(
            f"  FAK PARTIAL | all legs got fills, placing GTC for remainder..."
        )
        gtc_results = []
        for r in fak_results:
            remainder = size - r["fak_filled"]
            if remainder < 1:
                r["gtc_order_id"] = ""
                continue
            gtc_result = self._place_order(
                r["token_id"], "BUY", r["current_price"], remainder,
                OrderType.GTC,
            )
            if gtc_result["success"]:
                r["gtc_order_id"] = gtc_result["order_id"]
                log.info(
                    f"  GTC BUY | {r['description']} | "
                    f"{remainder:.1f} remaining @ {r['current_price']:.4f}"
                )
                self._record_orders(1)
            else:
                r["gtc_order_id"] = ""

        # 7. Adaptive pricing on GTC remainders
        gtc_orders = [r for r in fak_results if r.get("gtc_order_id")]
        if gtc_orders:
            watch_map = {r["gtc_order_id"]: size - r["fak_filled"]
                         for r in gtc_orders}
            self.fill_tracker.watch_orders(watch_map)

            log.info(
                f"  ADAPTIVE | {len(gtc_orders)} GTC orders, "
                f"initial wait {INITIAL_FILL_WAIT_S}s..."
            )
            self.fill_tracker.wait_for_fills(
                list(watch_map.keys()), timeout=INITIAL_FILL_WAIT_S
            )

            filled_gtc_ids = set()
            for r in gtc_orders:
                st = self.fill_tracker.get_status(r["gtc_order_id"])
                if st.get("filled"):
                    filled_gtc_ids.add(r["gtc_order_id"])

            # Adaptive bumps
            adaptive_deadline = time.time() + ADAPTIVE_MAX_TIME_S
            tick_bumps = 0

            while time.time() < adaptive_deadline:
                still_unfilled = [r for r in gtc_orders
                                  if r["gtc_order_id"] not in filled_gtc_ids]
                if not still_unfilled:
                    break

                tick_bumps += 1

                filled_cost = sum(r["current_price"] for r in fak_results
                                  if r.get("gtc_order_id", "") in filled_gtc_ids
                                  or r["fak_filled"] >= size - 0.01)
                unfilled_cost = sum(r["current_price"] for r in still_unfilled)
                remaining_budget = max_total_cost - filled_cost

                for r in still_unfilled:
                    meta = self.meta_cache.get(r["token_id"])
                    tick = float(meta["tick_size"])
                    new_price = r["current_price"] + tick

                    other_cost = unfilled_cost - r["current_price"]
                    if new_price + other_cost > remaining_budget:
                        continue

                    self._cancel_order(r["gtc_order_id"])
                    self.fill_tracker.unwatch([r["gtc_order_id"]])

                    remainder = size - r["fak_filled"]
                    new_result = self._place_order(
                        r["token_id"], "BUY", new_price, remainder,
                        OrderType.GTC,
                    )
                    if new_result["success"]:
                        log.info(
                            f"  ADAPTIVE #{tick_bumps} | {r['description']} | "
                            f"{r['current_price']:.4f} → {new_price:.4f}"
                        )
                        r["gtc_order_id"] = new_result["order_id"]
                        r["current_price"] = new_price
                        r["price"] = new_price
                        self.fill_tracker.watch_orders(
                            {new_result["order_id"]: remainder}
                        )
                        self._record_orders(1)

                current_ids = [r["gtc_order_id"] for r in gtc_orders
                               if r["gtc_order_id"] not in filled_gtc_ids]
                fill_status = self.fill_tracker.wait_for_fills(
                    current_ids, timeout=ADAPTIVE_TICK_INTERVAL_S
                )
                for r in gtc_orders:
                    if r["gtc_order_id"] in filled_gtc_ids:
                        continue
                    st = fill_status.get(r["gtc_order_id"], {})
                    if st.get("filled"):
                        filled_gtc_ids.add(r["gtc_order_id"])
                        log.info(
                            f"  FILLED | {r['description']} | "
                            f"@ {r['current_price']:.4f} (bump #{tick_bumps})"
                        )

            self.fill_tracker.unwatch(
                [r["gtc_order_id"] for r in gtc_orders]
            )

        # 8. Final evaluation
        all_filled = True
        confirmed = []
        unfilled = []
        for r in fak_results:
            total_filled = r["fak_filled"]
            # Check GTC fill via REST
            if r.get("gtc_order_id"):
                fill_info = self._verify_fill(r["gtc_order_id"])
                if fill_info:
                    total_filled += fill_info["size_matched"]

            if total_filled >= size - 0.01:
                confirmed.append({
                    "token_id": r["token_id"],
                    "description": r["description"],
                    "price": r["current_price"],
                    "size": total_filled,
                    "order_id": r.get("gtc_order_id", r["order_id"]),
                    "verified": True,
                    "fill_size": total_filled,
                })
            else:
                all_filled = False
                unfilled.append({
                    "token_id": r["token_id"],
                    "description": r["description"],
                    "price": r["current_price"],
                    "size": size,
                    "order_id": r.get("gtc_order_id", r["order_id"]),
                    "verified": False,
                    "fill_size": total_filled,
                })

        total_exec_ms = (time.time() - start_time) * 1000

        if all_filled:
            total_cost = sum(r["price"] * r["size"] for r in confirmed)
            log.info(
                f"EXEC OK    | {opp.asset} {opp.event_date} | "
                f"all {n_legs} legs filled in {total_exec_ms:.0f}ms | "
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
            self._log_execution(opp, size, fak_results, "FILLED",
                                total_exec_ms)
            self._record_success()
            return True
        else:
            # Partial — race with remaining GTC orders
            log.warning(
                f"EXEC PARTIAL | {opp.asset} {opp.event_date} | "
                f"{len(confirmed)}/{n_legs} complete after adaptive | "
                f"racing..."
            )
            self._race_unwind(confirmed, unfilled, size, opp, pair_key)
            self._log_execution(opp, size, fak_results, "PARTIAL_RACING",
                                total_exec_ms)
            self._record_failure(pair_key)
            return False

    # ------------------------------------------------------------------
    # Smart unwind: race buys vs sells after partial fill
    # ------------------------------------------------------------------
    def _race_unwind(self, filled_legs: list[dict],
                     unfilled_legs: list[dict], size: float,
                     opp, pair_key: str):
        """Handle partial fill: keep buying, defer sells.

        Strategy:
          - Keep unfilled buy GTC orders alive (they might still fill)
          - Do NOT post sells yet — give the buy a grace period
          - check_races() will post sells after RACE_SELL_GRACE_S (10 min)
          - If the buy fills within the grace period, no sells needed
        """
        race_key = f"{pair_key}|race"
        self.positions[race_key] = {
            "asset": opp.asset,
            "event_date": opp.event_date,
            "size": size,
            "entry_cost": sum(r["price"] * size for r in filled_legs),
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "race_started": time.time(),
            "legs": filled_legs,
            "status": "RACING",
            "buy_orders": [{"order_id": r["order_id"],
                            "token_id": r["token_id"],
                            "description": r["description"],
                            "price": r["price"],
                            "size": size} for r in unfilled_legs],
            "sell_orders": [],  # deferred — posted after grace period
            "sells_posted": False,
        }
        self._save_positions()

        log.info(
            f"  RACE STARTED | {len(unfilled_legs)} buys active | "
            f"sells deferred for {RACE_SELL_GRACE_S}s grace period | "
            f"check_races() will monitor"
        )

    # ------------------------------------------------------------------
    # Race monitoring: check racing positions every cycle
    # ------------------------------------------------------------------
    _last_race_check = 0.0  # class-level throttle

    def check_races(self):
        """Monitor RACING positions and adjust order sizes as fills come in.

        Called every cycle from the scanner loop, but throttled to run at
        most every 10 seconds (REST API calls are expensive).

        For each racing position:

        The race tracks the "uncovered size" — how many shares on the filled
        legs are not yet matched by the unfilled buy.

        As the buy partially fills:
          uncovered shrinks → cancel+replace sells with smaller size

        As sells partially fill:
          shares are gone → cancel+replace buy with smaller size

        Terminal states:
          - Buy fully fills → cancel all sells, position is complete
          - All shares sold → cancel buy, clean exit
          - Both sides converge to 0 → clean exit
        """
        # Throttle: only check every 10 seconds
        now = time.time()
        if now - self._last_race_check < 10:
            return
        self._last_race_check = now

        # Skip if no racing positions
        has_racing = any(
            p.get("status") == "RACING" for p in self.positions.values()
        )
        if not has_racing:
            return

        to_remove = []
        for pk, pos in list(self.positions.items()):
            if pos.get("status") != "RACING":
                continue

            buy_orders = pos.get("buy_orders", [])
            sell_orders = pos.get("sell_orders", [])
            target_size = pos["size"]

            # --- Measure current state ---

            # How much did the buy fill?
            buy_filled = 0.0
            for bo in buy_orders:
                fill_info = self._verify_fill(bo.get("order_id", ""))
                if fill_info:
                    bo["filled_size"] = fill_info["size_matched"]
                    buy_filled = max(buy_filled, fill_info["size_matched"])

            # What's the actual balance on each filled leg?
            leg_balances = {}
            for lg in pos.get("legs", []):
                leg_balances[lg["token_id"]] = self._check_balance(
                    lg["token_id"]
                )

            # Uncovered = target - buy_filled (what we still need to unwind)
            uncovered = max(0, target_size - buy_filled)

            # --- Terminal states ---

            # Buy fully filled → complete position
            if buy_filled >= target_size - 0.01:
                log.info(
                    f"RACE WON | {pos['asset']} {pos['event_date']} | "
                    f"buy filled {buy_filled:.1f}/{target_size:.0f} — "
                    f"cancelling sells, position complete!"
                )
                for so in sell_orders:
                    self._cancel_order(so.get("order_id", ""))
                base_key = pk.replace("|race", "")
                del self.positions[pk]
                self.positions[base_key] = {
                    "asset": pos["asset"],
                    "event_date": pos["event_date"],
                    "size": target_size,
                    "entry_cost": pos["entry_cost"],
                    "entry_time": pos["entry_time"],
                    "legs": pos["legs"] + [
                        {"token_id": bo["token_id"],
                         "description": bo.get("description", ""),
                         "price": bo["price"],
                         "size": bo.get("filled_size", target_size)}
                        for bo in buy_orders
                    ],
                }
                self._save_positions()
                self._record_success()
                continue

            # All shares sold (no balance left) → clean exit
            total_balance = sum(leg_balances.values())
            if total_balance < 0.5 and sell_orders:
                log.info(
                    f"RACE EXIT | {pos['asset']} {pos['event_date']} | "
                    f"all shares sold — cancelling buys, clean exit"
                )
                for bo in buy_orders:
                    self._cancel_order(bo.get("order_id", ""))
                to_remove.append(pk)
                continue

            # --- Post sells after grace period ---
            race_started = pos.get("race_started")
            if not race_started:
                # Backward compat: old positions don't have race_started.
                # Set it to now so the grace period starts fresh.
                race_started = now
                pos["race_started"] = race_started
            grace_elapsed = now - race_started
            if not pos.get("sells_posted") and grace_elapsed >= RACE_SELL_GRACE_S:
                log.info(
                    f"  RACE GRACE EXPIRED | {pos['asset']} {pos['event_date']} | "
                    f"{grace_elapsed:.0f}s elapsed — posting sells"
                )
                for lg in pos.get("legs", []):
                    balance = leg_balances.get(lg["token_id"], 0)
                    sell_size = min(uncovered, balance)
                    if sell_size < 0.5:
                        continue
                    try:
                        result = self._place_order(
                            lg["token_id"], "SELL", lg["price"], sell_size,
                            OrderType.GTC,
                        )
                        if result["success"]:
                            sell_orders.append({
                                "order_id": result["order_id"],
                                "token_id": lg["token_id"],
                                "description": lg.get("description", ""),
                                "price": lg["price"],
                                "size": sell_size,
                            })
                            log.info(
                                f"  RACE SELL | {lg.get('description', '')} | "
                                f"{sell_size:.2f} @ {lg['price']:.4f}"
                            )
                    except Exception as e:
                        log.error(
                            f"  RACE SELL ERROR | {lg.get('description', '')} | {e}"
                        )
                pos["sell_orders"] = sell_orders
                pos["sells_posted"] = True
                self._save_positions()
            elif not pos.get("sells_posted"):
                remaining = RACE_SELL_GRACE_S - grace_elapsed
                log.debug(
                    f"  RACE GRACE | {pos['asset']} {pos['event_date']} | "
                    f"{remaining:.0f}s remaining before sells"
                )

            # --- Adjust order sizes ---

            # Adjust sells: should match uncovered size, not original size
            changed = False
            for so in sell_orders:
                current_sell_size = so.get("size", 0)
                balance = leg_balances.get(so["token_id"], 0)
                # Sell size = min(uncovered, actual balance)
                new_sell_size = min(uncovered, balance)

                if new_sell_size < 0.5:
                    # Nothing to sell — cancel
                    self._cancel_order(so.get("order_id", ""))
                    so["size"] = 0
                    so["order_id"] = ""
                    changed = True
                elif abs(new_sell_size - current_sell_size) >= 0.5:
                    # Size changed significantly — cancel and re-place
                    self._cancel_order(so.get("order_id", ""))
                    result = self._place_order(
                        so["token_id"], "SELL", so["price"], new_sell_size,
                        OrderType.GTC,
                    )
                    if result["success"]:
                        log.info(
                            f"  RACE RESIZE SELL | {so.get('description', '')} | "
                            f"{current_sell_size:.1f} → {new_sell_size:.1f} | "
                            f"order_id={result['order_id']}"
                        )
                        so["order_id"] = result["order_id"]
                        so["size"] = new_sell_size
                    changed = True

            # Adjust buy: should match total remaining balance on filled legs
            # (if sells reduced the balance, buy should shrink too)
            for bo in buy_orders:
                current_buy_size = bo.get("size", 0)
                # New buy size = how many shares we still hold across filled legs
                new_buy_size = min(total_balance, target_size)

                if new_buy_size < 0.5:
                    self._cancel_order(bo.get("order_id", ""))
                    bo["size"] = 0
                    bo["order_id"] = ""
                    changed = True
                elif abs(new_buy_size - current_buy_size) >= 0.5:
                    self._cancel_order(bo.get("order_id", ""))
                    result = self._place_order(
                        bo["token_id"], "BUY", bo["price"], new_buy_size,
                        OrderType.GTC,
                    )
                    if result["success"]:
                        log.info(
                            f"  RACE RESIZE BUY | {bo.get('description', '')} | "
                            f"{current_buy_size:.1f} → {new_buy_size:.1f} | "
                            f"order_id={result['order_id']}"
                        )
                        bo["order_id"] = result["order_id"]
                        bo["size"] = new_buy_size
                    changed = True

            # Clean up fully-cancelled orders
            pos["sell_orders"] = [
                so for so in sell_orders if so.get("size", 0) >= 0.5
            ]
            pos["buy_orders"] = [
                bo for bo in buy_orders if bo.get("size", 0) >= 0.5
            ]

            if changed:
                self._save_positions()

        for pk in to_remove:
            del self.positions[pk]
            self._save_positions()

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

        # Get actual balances for each leg (fees reduce shares on buy).
        # If any leg has 0 balance, assume operator manually closed it.
        leg_balances = {}
        for lg in pos["legs"]:
            balance = self._check_balance(lg["token_id"])
            if balance < 0.5:
                log.info(
                    f"  SELL SKIP | {pos['asset']} {pos['event_date']} | "
                    f"{lg.get('description', '')} balance={balance:.1f} — "
                    f"assuming manually closed"
                )
                del self.positions[pair_key]
                self._save_positions()
                return
            leg_balances[lg["token_id"]] = balance

        # Place GTC sell orders concurrently at ob_manager bid prices.
        # Sell actual balance per leg, not the requested size.
        results = []
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=n_legs) as pool:
            futures = {}
            for lg in pos["legs"]:
                bid_price = bid_prices.get(lg["token_id"], 0.0)
                if bid_price <= 0:
                    continue
                sell_size = leg_balances[lg["token_id"]]
                fut = pool.submit(
                    self._place_order,
                    lg["token_id"], "SELL", bid_price, sell_size,
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
