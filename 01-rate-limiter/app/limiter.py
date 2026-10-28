"""Core rate-limiting logic, stored in Redis.

Design decision: every decision runs as ONE Lua script inside Redis.
Redis executes a script atomically (no other command runs in between), so
"load config -> read state -> refill -> check -> decrement -> write" can never
interleave with another request for the same key. That gives requirement 4
(no double-spend) without application-level locks, and it still holds when
several limiter processes share one Redis (the distributed-mode stretch goal).

It is also one network round trip per decision, which matters for throughput.

The script reads the clock with redis.call('TIME') instead of trusting the
caller's clock, so limiter instances on different machines agree on "now".
"""
from __future__ import annotations

import json
import uuid

from redis.asyncio import Redis

from .core import DEFAULT_CONFIG, Decision

DECIDE_LUA = """
-- KEYS: 1 bucket hash, 2 window zset, 3 config hash, 4 stats key prefix, 5 sequence counter
-- ARGV: 1 client, 2 cost, 3 unique member id, 4 default config (json)
local client = ARGV[1]
local cost   = tonumber(ARGV[2])

local cfg = cjson.decode(ARGV[4])
local raw = redis.call('HGET', KEYS[3], client)
if raw then
  for k, v in pairs(cjson.decode(raw)) do cfg[k] = v end
end

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

-- tostring() keeps only 14 significant digits, which would round a timestamp
-- like 1793185568.123456 to 0.1 ms. Always write numbers at full precision.
local function num(x) return string.format('%.17g', x) end

-- Position of this decision among all decisions for this client: the exact
-- execution order, which the load test replays (clocks can't be trusted for that).
local seq = redis.call('INCR', KEYS[5])

local allowed, remaining, retry_after, reset, limit = 0, 0, 0, 0, 0

if cfg.mode == 'sliding_window' then
  -- Sliding window log: one zset member per allowed request, scored by time.
  -- Drop everything older than the window, then count what is left.
  limit = tonumber(cfg.limit)
  local window = tonumber(cfg.window)
  -- Never let this client's clock run backwards (see the token bucket below).
  local newest = redis.call('ZRANGE', KEYS[2], -1, -1, 'WITHSCORES')
  if newest[2] and tonumber(newest[2]) > now then now = tonumber(newest[2]) end
  redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now - window)
  local count = redis.call('ZCARD', KEYS[2])
  if count + cost <= limit then
    for i = 1, cost do redis.call('ZADD', KEYS[2], now, ARGV[3] .. ':' .. i) end
    count = count + cost
    allowed = 1
  end
  redis.call('PEXPIRE', KEYS[2], math.ceil(window * 1000) + 1000)
  remaining = limit - count
  local oldest = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
  if oldest[2] then reset = math.max(0, tonumber(oldest[2]) + window - now) end
  if allowed == 0 then retry_after = reset end
else
  -- Token bucket with lazy refill: no timers, just "how long since we last looked?"
  local rate  = tonumber(cfg.rate)
  local burst = tonumber(cfg.burst)
  limit = burst
  local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
  local tokens = tonumber(data[1])
  local ts = tonumber(data[2])
  if tokens == nil then tokens = burst; ts = now end   -- new client: full bucket
  -- Redis's clock can step backwards a little (e.g. a VM syncing its time).
  -- Storing that earlier time would count the same milliseconds twice once the
  -- clock recovers, handing out extra tokens. So time only moves forward here.
  if now < ts then now = ts end
  tokens = math.min(burst, tokens + (now - ts) * rate)
  if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
  end
  redis.call('HSET', KEYS[1], 'tokens', num(tokens), 'ts', num(now))
  -- once the bucket would be full again, "no state" means the same thing
  redis.call('EXPIRE', KEYS[1], math.ceil(burst / rate) + 60)
  remaining = tokens
  reset = (burst - tokens) / rate
  if allowed == 0 then retry_after = (cost - tokens) / rate end
end

-- per-second counters for the live dashboard
local field = client .. (allowed == 1 and ':a' or ':d')
redis.call('HINCRBY', KEYS[4] .. t[1], field, 1)
redis.call('EXPIRE', KEYS[4] .. t[1], 120)
redis.call('HINCRBY', KEYS[4] .. 'total', field, 1)

-- Lua numbers become integers in replies, so floats travel as strings
return {allowed, num(remaining), num(retry_after), num(reset), num(now), limit, cfg.mode, seq}
"""


class RateLimiter:
    def __init__(self, redis: Redis, prefix: str = "rl:"):
        self.r = redis
        self.p = prefix
        self._decide = redis.register_script(DECIDE_LUA)
        self._default = json.dumps(DEFAULT_CONFIG)

    # ---- per-client configuration (requirements 2 and 5) -------------------
    async def get_config(self, client: str) -> dict:
        raw = await self.r.hget(f"{self.p}cfg", client)
        return {**DEFAULT_CONFIG, **json.loads(raw)} if raw else dict(DEFAULT_CONFIG)

    async def set_config(self, client: str, cfg: dict) -> dict:
        merged = {**DEFAULT_CONFIG, **cfg}
        await self.r.hset(f"{self.p}cfg", client, json.dumps(merged))
        await self.reset(client)  # new limits start from a clean bucket
        return merged

    async def list_configs(self) -> dict[str, dict]:
        raw = await self.r.hgetall(f"{self.p}cfg")
        return {k: {**DEFAULT_CONFIG, **json.loads(v)} for k, v in raw.items()}

    async def delete_config(self, client: str) -> None:
        await self.r.hdel(f"{self.p}cfg", client)
        await self.reset(client)

    async def reset(self, client: str) -> None:
        await self.r.delete(f"{self.p}tb:{client}", f"{self.p}sw:{client}")

    # ---- the decision (requirement 1) --------------------------------------
    async def check(self, client: str, cost: int = 1) -> Decision:
        allowed, remaining, retry_after, reset, now, limit, mode, seq = await self._decide(
            keys=[f"{self.p}tb:{client}", f"{self.p}sw:{client}", f"{self.p}cfg", f"{self.p}stats:",
                  f"{self.p}seq:{client}"],
            args=[client, cost, uuid.uuid4().hex, self._default],
        )
        return Decision(
            allowed=bool(int(allowed)),
            limit=int(limit),
            remaining=max(0.0, float(remaining)),
            retry_after=float(retry_after),
            reset=float(reset),
            mode=mode,
            server_time=float(now),
            seq=int(seq),
        )

    async def peek(self, client: str) -> dict:
        """Current bucket/window state without consuming anything (for the UI)."""
        cfg = await self.get_config(client)
        t = await self.r.time()
        now = t[0] + t[1] / 1e6
        if cfg["mode"] == "sliding_window":
            key = f"{self.p}sw:{client}"
            await self.r.zremrangebyscore(key, "-inf", now - float(cfg["window"]))
            used = await self.r.zcard(key)
            return {"mode": "sliding_window", "available": int(cfg["limit"]) - used, "capacity": int(cfg["limit"])}
        data = await self.r.hmget(f"{self.p}tb:{client}", "tokens", "ts")
        if data[0] is None:
            tokens = float(cfg["burst"])
        else:
            tokens = min(float(cfg["burst"]), float(data[0]) + max(0.0, now - float(data[1])) * float(cfg["rate"]))
        return {"mode": "token_bucket", "available": tokens, "capacity": int(cfg["burst"])}

    # ---- dashboard stats ----------------------------------------------------
    async def stats(self, seconds: int = 60) -> dict:
        t = await self.r.time()
        now = int(t[0])
        secs = list(range(now - seconds + 1, now + 1))
        pipe = self.r.pipeline()
        for s in secs:
            pipe.hgetall(f"{self.p}stats:{s}")
        pipe.hgetall(f"{self.p}stats:total")
        results = await pipe.execute()
        series: dict[str, list] = {}
        for i, s in enumerate(secs):
            for field, n in results[i].items():
                client, kind = field.rsplit(":", 1)
                row = series.setdefault(client, [{"t": x, "a": 0, "d": 0} for x in secs])
                row[i][kind] = int(n)
        totals: dict[str, dict] = {}
        for field, n in results[-1].items():
            client, kind = field.rsplit(":", 1)
            totals.setdefault(client, {"a": 0, "d": 0})[kind] = int(n)
        return {"now": now, "series": series, "totals": totals}

    async def ping(self) -> bool:
        return await self.r.ping()

    async def close(self) -> None:
        await self.r.aclose()
