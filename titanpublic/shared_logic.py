import functools
import os
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
    def __init__(self, exc: Exception):
        self.exc = exc


def cache(no_cache_exception: Optional[List[Exception]] = None):
    """Just functools cache, but with exception handling.
    
    If no_cache_exception is set, don't cache when the function returns this value
    """
    if not no_cache_exception:
        no_cache_exception = list()
    
    def _cache(func):
        _cache: Dict[Tuple[Any], Callable] = dict()

        @functools.wraps(func)
        def inner(*args, **kwargs):
            nonlocal _cache
            key = functools._make_key(args, kwargs, False)
            if key in _cache:
                result = _cache[key]
                if isinstance(result, ExceptionCacheWrapper):
                    raise result.exc
                return result

            try:
                value = func(*args, **kwargs)
                _cache[key] = value
                return value
            except Exception as e:
                if any([isinstance(e, E) for E in no_cache_exception]):
                    # Don't cache anything
                    raise e
                _cache[key] = ExceptionCacheWrapper(e)
                raise e

        return inner
    return _cache
