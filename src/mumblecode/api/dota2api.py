# coding=utf-8
import os
import tempfile

from requests import Session
from requests.adapters import HTTPAdapter

from mumblecode.caching import CacheWrapper, header_max_age_heuristic, SQLCache
from mumblecode.ratelimiting import RateLimiter


# requests session
_cache = SQLCache(os.path.join(tempfile.gettempdir(), "clamp", "dota2.db"))
_rate_limiter = RateLimiter((1, .003), (20, 1))
_requests_session = Session()
_adapter = HTTPAdapter(max_retries=5)
_requests_session.mount("http://", _adapter)
_requests_session.mount("https://", _adapter)
_session = CacheWrapper(_requests_session, _cache, header_max_age_heuristic,
                        limiter=_rate_limiter.hit, max_inflight=5)


# def get_match_history(**kwargs):
