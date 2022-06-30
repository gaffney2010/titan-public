import hashlib

Date = int
TeamName = str


def myhash(s):
    s = str(s)
    return int(hashlib.sha256(s.encode()).hexdigest(), 16) % (2**63)


def game_hash(away: TeamName, home: TeamName, date: Date):
    return myhash(away) ^ myhash(home) ^ myhash(date)
