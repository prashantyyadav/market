"""
upstox_bridge.py
================
Local WebSocket bridge for the Real-Time Options Screener.

Serves ws://localhost:8765. A browser client sends:

    {"action": "subscribe", "symbol": "NIFTY"}          # switch underlying
    {"action": "subscribe", "symbol": "NIFTY", "expiry": "2026-07-07"}

and receives a full live option-chain snapshot roughly once per second:

    {
      "type": "chain",
      "symbol": "NIFTY",
      "spot": 24005.85,
      "expiry": "2026-07-07",
      "expiries": ["2026-07-07", "2026-07-14", ...],
      "rows": [
        {"strike": 23850,
         "call": {"ltp": 246.25, "oi": 387595, "iv": 12.3, "bid": .., "ask": ..},
         "put":  {"ltp": 73.2,  "oi": 2426840, "iv": 11.8, "bid": .., "ask": ..}},
        ...
      ]
    }

DATA SOURCE
-----------
* With a valid Upstox access token, real option-chain data is polled via the
  official SDK (OptionsApi.get_put_call_option_chain) and pushed to clients.
* Without a token (or SDK), it falls back to a SIMULATED chain so the UI works.

SETUP
-----
    pip install websockets upstox-python-sdk
    export UPSTOX_ACCESS_TOKEN="your_daily_access_token"
    python upstox_bridge.py
"""

from __future__ import annotations  # allow "str | None" hints on Python 3.9

import asyncio
import json
import os
import random
from collections import Counter

import websockets

try:
    import upstox_client
    from upstox_client.rest import ApiException
    _SDK_AVAILABLE = True
except Exception:  # pragma: no cover
    upstox_client = None
    ApiException = Exception
    _SDK_AVAILABLE = False

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
HOST = "localhost"
PORT = 8765

ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()

CHAIN_POLL_INTERVAL = 1.0     # seconds between live option-chain fetches
SIM_TICK_INTERVAL = 1.0
STRIKES_EACH_SIDE = 10        # how many strikes above/below ATM to send

# Underlying symbol -> Upstox instrument key.
SYMBOL_TO_INSTRUMENT = {
    "NIFTY":     "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "FINNIFTY":  "NSE_INDEX|Nifty Fin Service",
    "CIPLA":     "NSE_EQ|INE059A01026",
    "RELIANCE":  "NSE_EQ|INE002A01018",
    "TCS":       "NSE_EQ|INE467B01029",
    "INFY":      "NSE_EQ|INE009A01021",
    "HDFCBANK":  "NSE_EQ|INE040A01034",
    "SBIN":      "NSE_EQ|INE062A01020",
}

# Fallback base spot + strike step for the simulator.
SYMBOL_SIM = {
    "NIFTY":     (24000.0, 50),
    "BANKNIFTY": (51500.0, 100),
    "FINNIFTY":  (23000.0, 50),
    "CIPLA":     (1476.0, 20),
    "RELIANCE":  (2950.0, 20),
    "TCS":       (3850.0, 50),
    "INFY":      (1650.0, 20),
    "HDFCBANK":  (1680.0, 20),
    "SBIN":      (820.0, 10),
}
DEFAULT_SIM = (1000.0, 20)


# --------------------------------------------------------------------------- #
# Live option-chain provider (Upstox REST, run in a thread pool)
# --------------------------------------------------------------------------- #
class OptionChainProvider:
    def __init__(self, access_token: str):
        cfg = upstox_client.Configuration()
        cfg.access_token = access_token
        client = upstox_client.ApiClient(cfg)
        self._api = upstox_client.OptionsApi(client)
        self._search = upstox_client.InstrumentsApi(client)
        self._quote = upstox_client.MarketQuoteV3Api(client)
        self._expiry_cache = {}   # instrument_key -> [expiry strings]
        self._key_cache = {}      # symbol -> instrument_key

    def resolve_instrument(self, symbol: str) -> str:
        """Map a user symbol to the best NSE underlying instrument key.

        Works for ANY stock or index (F&O or not). Prefers F&O-enabled
        underlyings (so options show when available), then exact ticker
        matches, then any NSE equity/index match.
        """
        symbol = symbol.upper().strip()
        if symbol in SYMBOL_TO_INSTRUMENT:
            return SYMBOL_TO_INSTRUMENT[symbol]
        if symbol in self._key_cache:
            return self._key_cache[symbol]

        resp = self._search.search_instrument(symbol)
        results = resp.data or []

        def seg(d):
            return (d.get("segment") or "").upper()

        def tsym(d):
            return (d.get("trading_symbol") or "").upper()

        ordered = []
        seen = set()

        def add(items):
            for d in items:
                key = d.get("instrument_key")
                if key and key not in seen and seg(d) in ("NSE_EQ", "NSE_INDEX"):
                    seen.add(key)
                    ordered.append(key)

        # 1) F&O-enabled underlyings referenced by option/future contracts.
        fo_underlyings = {
            d.get("underlying_key") for d in results
            if seg(d) == "NSE_FO" and d.get("underlying_key")
        }
        add([d for d in results if d.get("instrument_key") in fo_underlyings])
        # 2) Exact ticker matches (index first, then equity).
        exact = [d for d in results if tsym(d) == symbol]
        add([d for d in exact if seg(d) == "NSE_INDEX"])
        add([d for d in exact if seg(d) == "NSE_EQ"])
        # 3) Any remaining NSE index / equity match.
        add([d for d in results if seg(d) == "NSE_INDEX"])
        add([d for d in results if seg(d) == "NSE_EQ"])

        if not ordered:
            raise RuntimeError(f"No NSE stock/index found for '{symbol}'")

        key = ordered[0]
        self._key_cache[symbol] = key
        print(f"[live] Resolved {symbol} -> {key}")
        return key

    def has_options(self, instrument_key: str) -> bool:
        try:
            return bool(self.expiries(instrument_key))
        except RuntimeError:
            return False

    def ltp(self, instrument_key: str) -> float:
        """Last traded price for any instrument (used for non-F&O stocks)."""
        resp = self._quote.get_ltp(instrument_key=instrument_key)
        data = resp.data or {}
        for v in data.values():
            price = getattr(v, "last_price", None)
            if price is not None:
                return float(price)
        return 0.0

    def expiries(self, instrument_key: str):
        if instrument_key not in self._expiry_cache:
            try:
                resp = self._api.get_option_contracts(instrument_key)
                exps = sorted({d.expiry.strftime("%Y-%m-%d") for d in resp.data})
            except ApiException as e:
                # Stock/index has no listed options (not in F&O).
                raise RuntimeError("No options available for this symbol (not in F&O).") from e
            self._expiry_cache[instrument_key] = exps
        return self._expiry_cache[instrument_key]

    def chain(self, symbol: str, expiry):
        """Fetch the live chain. Returns a dict payload ready for the client.

        For F&O symbols this is the real option chain. For cash-only stocks
        (no options), it returns a SYNTHETIC chain: live LTP as spot plus a
        generated strike grid with intrinsic values (no premiums/OI/IV).
        """
        instrument_key = self.resolve_instrument(symbol)

        if not self.has_options(instrument_key):
            return self._synthetic_chain(symbol, instrument_key)

        exps = self.expiries(instrument_key)
        if not exps:
            raise RuntimeError(f"No expiries for {symbol}")
        if expiry not in exps:
            expiry = exps[0]

        resp = self._api.get_put_call_option_chain(instrument_key, expiry)
        data = resp.data or []

        spot = data[0].underlying_spot_price if data else 0.0

        # Build a UNIFORM strike grid so spacing is always consistent.
        # NSE far/monthly expiries mix intervals (50, 100, ...); we detect the
        # tightest interval (the standard ATM step) and keep only strikes that
        # sit exactly on that grid around the ATM strike.
        data_sorted = sorted(data, key=lambda r: r.strike_price)
        if data_sorted:
            all_strikes = [r.strike_price for r in data_sorted]
            diffs = [round(all_strikes[i + 1] - all_strikes[i], 2)
                     for i in range(len(all_strikes) - 1)]
            diffs = [d for d in diffs if d > 0]
            # Use the MOST COMMON gap as the standard step. This ignores odd
            # corporate-action-adjusted strikes (e.g. a stray 292.0 among a
            # 2.5-spaced chain) and keeps the grid uniform.
            step = Counter(diffs).most_common(1)[0][0] if diffs else 0

            atm_idx = min(range(len(data_sorted)),
                          key=lambda i: abs(data_sorted[i].strike_price - spot))
            atm_strike = data_sorted[atm_idx].strike_price

            if step > 0:
                by_strike = {round(r.strike_price, 2): r for r in data_sorted}

                def find(target):
                    # tolerant lookup for float strikes
                    for s, r in by_strike.items():
                        if abs(s - target) < 0.01:
                            return r
                    return None

                # Walk outward from ATM on an exact `step` grid, stopping on the
                # first missing strike so spacing stays perfectly uniform.
                selected = [by_strike[round(atm_strike, 2)]] if round(atm_strike, 2) in by_strike else []
                for i in range(1, STRIKES_EACH_SIDE + 1):
                    up = find(atm_strike + i * step)
                    if up is None:
                        break
                    selected.append(up)
                for i in range(1, STRIKES_EACH_SIDE + 1):
                    dn = find(atm_strike - i * step)
                    if dn is None:
                        break
                    selected.append(dn)

                data_sorted = sorted(selected, key=lambda r: r.strike_price) or data_sorted
            else:
                lo = max(0, atm_idx - STRIKES_EACH_SIDE)
                hi = min(len(data_sorted), atm_idx + STRIKES_EACH_SIDE + 1)
                data_sorted = data_sorted[lo:hi]

        rows = []
        for r in data_sorted:
            rows.append({
                "strike": r.strike_price,
                "call": _leg(r.call_options),
                "put": _leg(r.put_options),
            })

        return {
            "type": "chain",
            "symbol": symbol,
            "spot": round(spot, 2),
            "expiry": expiry,
            "expiries": exps,
            "rows": rows,
        }

    def _synthetic_chain(self, symbol: str, instrument_key: str):
        """Chain for cash-only stocks: real LTP + generated intrinsic grid."""
        spot = self.ltp(instrument_key)

        # Choose a sensible strike step based on price magnitude.
        if spot >= 20000:
            step = 100
        elif spot >= 5000:
            step = 50
        elif spot >= 1000:
            step = 20
        elif spot >= 250:
            step = 5
        elif spot >= 50:
            step = 2.5
        else:
            step = 1
        atm = round(spot / step) * step

        rows = []
        for i in range(-STRIKES_EACH_SIDE, STRIKES_EACH_SIDE + 1):
            strike = round(atm + i * step, 2)
            if strike <= 0:
                continue
            # No options exist -> report intrinsic value only.
            rows.append({
                "strike": strike,
                "call": {"ltp": round(max(0.0, spot - strike), 2), "oi": 0, "bid": 0, "ask": 0, "iv": 0},
                "put": {"ltp": round(max(0.0, strike - spot), 2), "oi": 0, "bid": 0, "ask": 0, "iv": 0},
            })

        return {
            "type": "chain",
            "symbol": symbol,
            "spot": round(spot, 2),
            "expiry": "CASH (no options)",
            "expiries": ["CASH (no options)"],
            "rows": rows,
            "cash_only": True,
        }


def _leg(opt):
    """Extract the fields we care about from a call/put option leg."""
    md = getattr(opt, "market_data", None)
    gk = getattr(opt, "option_greeks", None)

    def num(v):
        try:
            return round(float(v), 2)
        except (TypeError, ValueError):
            return 0.0

    return {
        "ltp": num(getattr(md, "ltp", 0)),
        "oi": num(getattr(md, "oi", 0)),
        "bid": num(getattr(md, "bid_price", 0)),
        "ask": num(getattr(md, "ask_price", 0)),
        "iv": num(getattr(gk, "iv", 0)) if gk else 0.0,
    }


# --------------------------------------------------------------------------- #
# Simulated option-chain fallback
# --------------------------------------------------------------------------- #
class SimChain:
    def __init__(self):
        self._spot = {}

    def chain(self, symbol: str, expiry: str | None):
        base, step = SYMBOL_SIM.get(symbol, DEFAULT_SIM)
        spot = self._spot.get(symbol, base)
        spot = round(max(1.0, spot + random.uniform(-base * 0.001, base * 0.001)), 2)
        self._spot[symbol] = spot

        atm = round(spot / step) * step
        rows = []
        for i in range(-STRIKES_EACH_SIDE, STRIKES_EACH_SIDE + 1):
            strike = atm + i * step
            if strike <= 0:
                continue
            call_intr = max(0.0, spot - strike)
            put_intr = max(0.0, strike - spot)
            # crude time value so premiums look alive
            tv = max(2.0, step * 0.8 * random.uniform(0.6, 1.2))
            rows.append({
                "strike": strike,
                "call": {"ltp": round(call_intr + tv, 2), "oi": random.randint(10000, 500000),
                         "bid": 0, "ask": 0, "iv": round(random.uniform(10, 20), 2)},
                "put": {"ltp": round(put_intr + tv, 2), "oi": random.randint(10000, 500000),
                        "bid": 0, "ask": 0, "iv": round(random.uniform(10, 20), 2)},
            })
        return {
            "type": "chain", "symbol": symbol, "spot": spot,
            "expiry": expiry or "SIMULATED",
            "expiries": ["SIMULATED"], "rows": rows,
        }


# Chosen provider (set in main()).
PROVIDER = None
IS_LIVE = False


# --------------------------------------------------------------------------- #
# Client handling
# --------------------------------------------------------------------------- #
class ClientState:
    def __init__(self):
        self.symbol = None      # set by the client's first subscribe
        self.expiry = None


async def fetch_chain(symbol: str, expiry: str | None):
    """Run the (blocking) provider call off the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, PROVIDER.chain, symbol, expiry)


async def receive_commands(websocket, state: ClientState):
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if msg.get("action") == "subscribe" and msg.get("symbol"):
                sym = msg["symbol"].upper().strip()
                state.symbol = sym
                state.expiry = msg.get("expiry")  # may be None -> nearest
                print(f"[bridge] Client -> {state.symbol} exp={state.expiry}")
    except websockets.ConnectionClosed:
        pass


async def stream_chain(websocket, state: ClientState):
    interval = CHAIN_POLL_INTERVAL if IS_LIVE else SIM_TICK_INTERVAL
    while True:
        # Wait until the client has told us which symbol it wants.
        if not state.symbol:
            await asyncio.sleep(0.1)
            continue
        symbol, expiry = state.symbol, state.expiry
        try:
            payload = await fetch_chain(symbol, expiry)
            # Only lock in the resolved expiry if the client hasn't switched
            # symbols while we were fetching.
            if state.symbol == symbol:
                state.expiry = payload.get("expiry")
            await websocket.send(json.dumps(payload))
        except websockets.ConnectionClosed:
            break
        except ApiException as e:
            await websocket.send(json.dumps({
                "type": "error",
                "message": f"Upstox API {e.status}: check token/IP allowlist.",
            }))
            print(f"[bridge] API error: {e.status} {getattr(e,'body','')}")
            await asyncio.sleep(2)
        except RuntimeError as e:
            # e.g. symbol not found or has no options.
            await websocket.send(json.dumps({"type": "error", "message": str(e)}))
            print(f"[bridge] {e}")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"[bridge] chain error: {type(e).__name__}: {e}")
            await asyncio.sleep(1)
        await asyncio.sleep(interval)


async def handler(websocket):
    peer = getattr(websocket, "remote_address", "unknown")
    print(f"[bridge] Client connected: {peer} (mode={'LIVE' if IS_LIVE else 'SIM'})")
    state = ClientState()
    receiver = asyncio.create_task(receive_commands(websocket, state))
    streamer = asyncio.create_task(stream_chain(websocket, state))
    try:
        _, pending = await asyncio.wait(
            {receiver, streamer}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
    finally:
        print(f"[bridge] Client disconnected: {peer}")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
async def main():
    global PROVIDER, IS_LIVE

    if ACCESS_TOKEN and _SDK_AVAILABLE:
        try:
            PROVIDER = OptionChainProvider(ACCESS_TOKEN)
            IS_LIVE = True
            print("[bridge] LIVE mode: real Upstox option chain.")
        except Exception as e:
            PROVIDER = SimChain()
            IS_LIVE = False
            print(f"[bridge] Live init failed ({e}); using SIMULATED chain.")
    else:
        PROVIDER = SimChain()
        IS_LIVE = False
        reason = "no UPSTOX_ACCESS_TOKEN" if not ACCESS_TOKEN else "SDK missing"
        print(f"[bridge] SIMULATED mode ({reason}).")

    print(f"[bridge] WebSocket server on ws://{HOST}:{PORT}  (Ctrl+C to stop)")
    async with websockets.serve(handler, HOST, PORT):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[bridge] Shutting down.")
