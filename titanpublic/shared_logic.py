import functools
import os
from typing import Optional

from frozendict import frozendict
import yaml


@functools.lru_cache(1)
def get_secrets(dir: Optional[str] = None):
    with open(os.path.join(dir, "secrets.yaml"), "r") as f:
        secrets = yaml.load(f, Loader=yaml.Loader)
    secrets = frozendict(secrets)
    return secrets
