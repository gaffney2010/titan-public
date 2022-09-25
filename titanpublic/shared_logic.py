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
        self.exc_type = type(exc)
        self.exc_str = str(exc)


def cache(no_cache_exception: Optional[List[Exception]] = None):
    """Just functools cache, but with exception handling.
    
    If no_cache_exception is set, don't cache when the function returns this value
    """
    if not no_cache_exception:
        no_cache_exception = list()
    
    def _cache(func):
        __cache: Dict[Tuple[Any], Callable] = dict()

        @functools.wraps(func)
        def inner(*args, **kwargs):
            nonlocal __cache
            key = functools._make_key(args, kwargs, False)
            if key in __cache:
                result = __cache[key]
                if isinstance(result, ExceptionCacheWrapper):
                    raise result.exc_type(result.exc_str)
                return result

            try:
                value = func(*args, **kwargs)
                __cache[key] = value
                return value
            except Exception as e:
                if any([isinstance(e, E) for E in no_cache_exception]):
                    # Don't cache anything
                    raise e
                __cache[key] = ExceptionCacheWrapper(e)
                raise e

        return inner
    return _cache
