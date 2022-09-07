import hashlib

from . import shared_types


def myhash(s):
    s = str(s)
    return int(hashlib.sha256(s.encode()).hexdigest(), 16) % (2**63)


def game_hash(
    away: shared_types.TeamName, home: shared_types.TeamName, date: shared_types.Date
):
    return myhash(away) ^ myhash(home) ^ myhash(date)
