import attr
import collections
import json
import logging
import functools
from typing import Any, Dict, Tuple

import MySQLdb
import pandas as pd

Date = int
Team = str


# TODO: I don't think I need this class anymore.
@attr.s(frozen=True)
class Game(object):
    away: Team = attr.ib()
    home: Team = attr.ib()
    date: Date = attr.ib()
    neutral: int = attr.ib()
    winner: Team = attr.ib()
    game_hash: int = attr.ib()
    timestamp: int = attr.ib()


def update_feature(
    db_name: str,
    feature: str,
    game_hash: int,
    input_timestamp: int,
    payload: Dict[str, Any],
    secrets: Dict[str, Any],
) -> None:
    """Update a feature in Titan.

    Looks up DB connection details from `secrets.yaml`.  Looks in the db_name database.
    It then updates the game with the new payload / input_timestamp.

    Args:
        db_name: The database to look in, usually the name of the sport.
        feature: The feature we want to update
        game_hash: References the game.
        input_timestamp: Max of upstream inputs' write time, for cache invalidation.
        payload: A dict with a top-level field called `value`
        secrets: Contains AWS login info.
    """

    host = secrets["aws_host"]
    port = 3306
    dbname = db_name
    user = secrets["aws_username"]
    password = secrets["aws_password"]

    value = "NULL"
    if "value" in payload:
        value = payload["value"]
    payload = json.dumps(payload)

    with MySQLdb.connect(
        host=host, port=port, user=user, passwd=password, db=dbname
    ) as con:
        cur = con.cursor()
        cur.execute(f"""
            REPLACE INTO {feature} (game_hash, value, payload, input_timestamp, output_timestamp)
            VALUES ({game_hash}, {value}, '{payload}', {input_timestamp}, UNIX_TIMESTAMP(NOW()));
        """
        )
        con.commit()


@functools.lru_cache()
def pull_data(
    db_name: str,
    features: Tuple[str, ...],
    min_date: int,
    max_date: int,
    secrets: Dict[str, Any],
    pull_payload: bool = False,
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
        pull_payload: If true, pulls entire payload for a feature, a json with 
            potentially auxillary info.  Otherwise returns a single value representing
            the feature.
    
    Returns:
        df: The results in a dataframe.
        max_timestamp: The maximum timestamp over all consumed data.  Needed for titan.
    """
    max_timestamp = 0
    target_field = "payload" if pull_payload else "value"

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
            FROM {db_name}.games
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
            feature_values["game_hash"].append(game.game_hash)
            feature_values["timestamp"].append(game.timestamp)
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
