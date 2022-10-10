import collections
import json
import logging
import traceback
from typing import Any, Dict, Tuple

import attr
import MySQLdb
import pandas as pd

from . import hash
from . import shared_types


# TODO: Return success / failure
def update_feature(
    db_name: str,
    feature: str,
    game_hash: int,
    input_timestamp: str,
    payload: Dict[str, Any],
    secrets: Dict[str, Any],
) -> int:
    """Update a feature in Titan.

    Looks up DB connection details from `secrets.yaml`.  Looks in the db_name database.
    It then updates the game with the new payload / input_timestamp.

    Args:
        db_name: The database to look in, usually the name of the sport.
        feature: The feature we want to update
        game_hash: References the game.
        input_timestamp: Max of upstream inputs' write time, for cache invalidation.
            This is string encoded, and may be multiple timestamps separated with a
            comma.
        payload: A dict with a top-level field called `value`
        secrets: Contains AWS login info.

    Returns:
        output_timestamp written with new record
    """

    host = secrets["aws_host"]
    port = 3306
    dbname = db_name
    user = secrets["aws_username"]
    password = secrets["aws_password"]

    value = "NULL"
    if "value" in payload:
        value = payload["value"]
        if isinstance(value, str):
            value = "'" + value + "'"
    payload = json.dumps(payload)

    if input_timestamp.find(",") != -1:
        input_timestamp = str(max([int(x) for x in input_timestamp.split(",")]))

    with MySQLdb.connect(
        host=host, port=port, user=user, passwd=password, db=dbname
    ) as con:
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT input_timestamp from {feature} where game_hash = {game_hash};
        """
        )
        timestamp = cur.fetchone()
        if timestamp is not None:
            timestamp = timestamp[0]
        if timestamp is not None and int(timestamp) > int(input_timestamp):
            # Handle some weird race condition by failing here
            return
        cur.execute("SELECT UNIX_TIMESTAMP(NOW());")
        new_timestamp = cur.fetchone()[0]

        if not game_hash:
            raise Exception("Invalid game_hash on titan write")
        if value is None:
            raise Exception("Invalid value on titan write")
        if payload is None:
            raise Exception("Invalid payload on titan write")
        if input_timestamp is None:
            raise Exception("Invalid input_ts on titan write")
        cur.execute(
            f"""
            REPLACE INTO {feature} (game_hash, value, payload, input_timestamp, output_timestamp)
            VALUES ({game_hash}, {value}, '{payload}', {input_timestamp}, {new_timestamp});
        """
        )
        con.commit()

    return int(new_timestamp)


# Cache on call side if you want a cache.
# @functools.lru_cache()
def pull_single_game(
    db_name: str,
    features: Tuple[str, ...],
    away: shared_types.TeamName,
    home: shared_types.TeamName,
    date: shared_types.Date,
    secrets: Dict[str, Any],
    pull_payload: bool = False,
) -> Tuple[Dict[str, Any], int]:
    """Pull a single game from Titan's DB.

    Looks up DB connection details from `secrets.yaml`.  Looks in the db_name database.
    It pulls base data (except winner), and any features passed.

    Args:
        db_name: The database to look in, usually the name of the sport.
        features: The non-base features, we want to pull.  These are the table names
            in the database.
        away: The away team.
        home: The home team.
        date: The date of the game we want to pull.
        secrets: Contains AWS login info.
        pull_payload: If true, pulls entire payload for a feature, a json with
            potentially auxillary info.  Otherwise returns a single value representing
            the feature.

    Returns:
        The variables for the game in a dict.
        input_timestamp: The input_timestamp
    """
    target_field = "payload" if pull_payload else "value"
    game_hash = hash.game_hash(away, home, date)

    host = secrets["aws_host"]
    port = 3306
    dbname = db_name
    user = secrets["aws_username"]
    password = secrets["aws_password"]

    with MySQLdb.connect(
        host=host, port=port, user=user, passwd=password, db=dbname
    ) as con:
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT away, home, date, neutral, winner, game_hash, timestamp
            FROM {db_name}.games
            WHERE game_hash = {game_hash};
            """
        )
        away, home, date, neutral, _, game_hash, timestamp = cur.fetchone()

        feature_values = dict()
        feature_values["away"] = away
        feature_values["home"] = home
        feature_values["date"] = date
        feature_values["neutral"] = neutral
        feature_values["game_hash"] = game_hash
        for feature in features:
            try:
                cur = con.cursor()
                cur.execute(
                    f"""
                    SELECT {target_field}, output_timestamp
                    FROM {feature}
                    WHERE game_hash = {game_hash};
                    """
                )
                value, output_timestamp = cur.fetchone()
            except:
                logging.debug(traceback.format_exc())
                value, output_timestamp = None, 0
            feature_values[feature] = value
            timestamp = max(timestamp, output_timestamp)

    return feature_values, timestamp


# @functools.lru_cache()
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

    column_names = [
        "away",
        "home",
        "date",
        "neutral",
        "winner",
        "game_hash",
        "timestamp",
    ]
    keep_column_names = [
        "away",
        "home",
        "date",
        "neutral",
        "winner",
        "game_hash",
    ]
    ts_columns = ["timestamp"]

    feature_field_names = list()
    for feature in features:
        feature_field_names.append(
            f"""
            {feature}.{target_field} AS {feature},
            {feature}.output_timestamp as {feature}_ts, 
        """
        )
        column_names.extend([feature, f"{feature}_ts"])
        keep_column_names.append(feature)
        ts_columns.append(f"{feature}_ts")
    feature_field_names.append("1 AS const")  # Trailing comma
    column_names.append("const")
    feature_field_clause = "".join(feature_field_names)

    feature_joins = list()
    for feature in features:
        feature_joins.append(
            f"""
            LEFT JOIN {db_name}.{feature} AS {feature}
            ON games.game_hash = {feature}.game_hash
        """
        )
    feature_join_clause = "".join(feature_joins)

    sql_query = f"""
        SELECT away, home, date, neutral, winner, games.game_hash, timestamp,
            {feature_field_clause}
        FROM {db_name}.games AS games
        {feature_join_clause}
        WHERE date >= {min_date} AND date < {max_date};
        """

    host = secrets["aws_host"]
    port = 3306
    dbname = db_name
    user = secrets["aws_username"]
    password = secrets["aws_password"]
    with MySQLdb.connect(
        host=host, port=port, user=user, passwd=password, db=dbname
    ) as con:
        pd_query = pd.read_sql_query(sql_query, con)
        df = pd.DataFrame(pd_query, columns=column_names)

    max_timestamp = 0
    for col in ts_columns:
        max_timestamp = max(max_timestamp, df[col].max())

    return df[keep_column_names], max_timestamp


def pull_data_multi_range(
    db_name: str,
    features: Tuple[str, ...],
    multi_range: shared_types.MultiRange,
    secrets: Dict[str, Any],
    pull_payload: bool = False,
) -> Tuple[pd.DataFrame, int]:
    dfs, tss = list(), list()
    for st, en in multi_range.ranges:
        df, ts = pull_data(
            db_name,
            features,
            st,
            en,
            secrets,
            pull_payload=pull_payload,
        )
        dfs.append(df)
        tss.append(ts)

    result_df = pd.concat(dfs, ignore_index=True)
    result_ts = max(tss)

    return result_df, result_ts
