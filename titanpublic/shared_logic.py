import functools
import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from frozendict import frozendict
import yaml


@functools.lru_cache(1)
def get_secrets(dir: Optional[str] = None):
    with open(os.path.join(dir, "secrets.yaml"), "r") as f:
        secrets = yaml.load(f, Loader=yaml.Loader)
    secrets = frozendict(secrets)
    return secrets


class ExceptionCacheWrapper(object):
    def __init__(self, exc: Exception, expiry: Optional[int]):
        self.exc_type = type(exc)
        self.exc_str = str(exc)
        self.expiry = expiry


def cache(
    no_cache_exception: Optional[List[Exception]] = None,
    ttl_cache_exception: Optional[Dict[Exception, int]] = None,
):
    """Just functools cache, but with exception handling.

    If no_cache_exception is set, don't cache when the function returns this value
    If ttl_cache_exception is set, this looks up ttl (in seconds) by exception class
        keys.  After expiration, the function will run again.
    """
    if not no_cache_exception:
        no_cache_exception = list()
    if not ttl_cache_exception:
        ttl_cache_exception = dict()

    def _cache(func):
        __cache: Dict[Tuple[Any], Callable] = dict()

        @functools.wraps(func)
        def inner(*args, **kwargs):
            nonlocal __cache
            key = functools._make_key(args, kwargs, False)
            if key in __cache:
                result = __cache[key]
                if isinstance(result, ExceptionCacheWrapper):
                    if result.expiry and result.expiry <= time.monotonic():
                        pass
                    else:
                        raise result.exc_type(result.exc_str)
                else:
                    return result

            try:
                value = func(*args, **kwargs)
                __cache[key] = value
                return value
            except Exception as e:
                if any([isinstance(e, E) for E in no_cache_exception]):
                    # Don't cache anything
                    raise e
                expiry = None
                for E, ttl in ttl_cache_exception.items():
                    if isinstance(e, E):
                        expiry = time.monotonic() + ttl
                        break
                __cache[key] = ExceptionCacheWrapper(e, expiry)
                raise e

        return inner

    return _cache
