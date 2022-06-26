import attr
import collections
import logging
import functools
import os
from typing import Any, Dict, Tuple

import MySQLdb
import pandas as pd

Date = int
Team = str


@attr.s(frozen=True)
class Game(object):
    away: Team = attr.ib()
    home: Team = attr.ib()
    date: Date = attr.ib()
    neutral: int = attr.ib()
    winner: Team = attr.ib()
    game_hash: int = attr.ib()
    timestamp: int = attr.ib()


@functools.lru_cache()
def pull_data(
    db_name: str,
    features: Tuple[str, ...],
    min_date: int,
    max_date: int,
    secrets: Dict[str, Any],
    payload: bool = False,
) -> Tuple[pd.DataFrame, int]:
    """Pull data from Titan's DB.

    Looks up DB connection details from `secrets.yaml`.  Looks in the db_name database.
    It pulls base data, and any features passed.  It only pulls games between the given
    dates.

    Args:
        db_name: The database to look in, usually the name of the sport.
        features: The non-base features, we want to pull.  These are the table names
            in the database.
        min_date: The minimum date to pull, inclusive.
        max_date: The maximum date to pull, exclusive.
        secrets: Contains AWS login info.
        payload: If true, pulls entire payload for a feature, a json with potentially
            auxillary info.  Otherwise returns a single value representing the feature.
    
    Returns:
        df: The results in a dataframe.
        max_timestamp: The maximum timestamp over all consumed data.  Needed for titan.
    """
    max_timestamp = 0
    target_field = "payload" if payload else "value"

    host = secrets["aws_host"]
    port = 3306
    dbname = db_name
    user = secrets["aws_username"]
    password = secrets["aws_password"]

    with MySQLdb.connect(
        host=host, port=port, user=user, passwd=password, db=dbname
    ) as con:
        games = list()
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT away, home, date, neutral, winner, game_hash, timestamp
            FROM games
            WHERE date >= {min_date} AND date < {max_date};
            """
        )
        for row in cur.fetchall():
            away, home, date, neutral, winner, game_hash, timestamp = row
            games.append(
                Game(
                    away=away,
                    home=home,
                    date=date,
                    neutral=neutral,
                    winner=winner,
                    game_hash=game_hash,
                    timestamp=timestamp,
                )
            )
            max_timestamp = max(max_timestamp, timestamp)

        logging.debug(
            f"Building fresh csv with features={features} and date_range=({min_date}, {max_date})"
        )
        feature_values = collections.defaultdict(list)
        for game in games:
            feature_values["away"].append(game.away)
            feature_values["home"].append(game.home)
            feature_values["date"].append(game.date)
            feature_values["neutral"].append(game.neutral)
            feature_values["winner"].append(game.winner)
            for feature in features:
                try:
                    cur = con.cursor()
                    cur.execute(
                        f"""
                        SELECT {target_field}, output_timestamp
                        FROM {feature}
                        WHERE game_hash={game.game_hash}
                    """
                    )
                    value, output_timestamp = cur.fetchone()
                except:
                    value, output_timestamp = None, 0
                feature_values[feature].append(value)
                max_timestamp = max(max_timestamp, output_timestamp)

    df = pd.DataFrame(feature_values)
    return df, max_timestamp
