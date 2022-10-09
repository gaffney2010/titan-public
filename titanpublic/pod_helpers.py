"""This contains some logic that helps with all the model pods."""

import logging
import os
import ssl
import time
import traceback
from typing import Any, Callable, Dict, Optional

import attr
import pika
import retrying

from . import hash, pull_data, shared_logic, shared_types


PREFETCH_COUNT = 100  # Minibatch size
ROLLOVER_WAIT_SEC = 3  # How long to wait before restarting on a Rabbit timeout
BIGGER_WAIT_SEC = 240  # When there's a node outage

MessageCallback = Callable[
    [
        str,
        str,
        shared_types.TeamName,
        shared_types.TeamName,
        shared_types.Date,
        int,
        str,
    ],
    Dict[str, Any],
]


@attr.s(frozen=True)
class TitanConfig(object):
    sport: str = attr.ib()
    env: str = attr.ib()
    secrets_dir: str = attr.ib()
    # Don't include any suffixes
    inbound_channel: str = attr.ib()
    outbound_channel: str = attr.ib()
    suffixes: Optional[str] = attr.ib(default=None)


def routing_key_resolver(id: str, sport: str, env: str, suffix: str = "") -> str:
    dev_suffix = ""
    if "dev" == env:
        dev_suffix = "-dev"

    if suffix:
        suffix = f"-{suffix}"

    return f"{id}-{sport}{dev_suffix}{suffix}"


def database_resolver(sport: str, env: str) -> str:
    dev_suffix = ""
    if "dev" == env:
        dev_suffix = "_dev"

    return f"{sport}{dev_suffix}"


def notify_titan(
    input_body: str,
    output_timestamp: int,
    status: str,
    titan_config: TitanConfig,
    channel,
) -> None:
    output_body = " ".join([input_body, str(output_timestamp), status,])
    channel.basic_publish(
        exchange="",
        routing_key=routing_key_resolver(
            titan_config.outbound_channel, titan_config.sport, titan_config.env
        ),
        body=output_body,
        properties=pika.BasicProperties(delivery_mode=1),
    )


def process_message(
    body: str, callback: MessageCallback, titan_config: TitanConfig, channel
) -> None:
    (sport, model_name, input_timestamp, away, home, date, neutral,) = body.split()
    date = int(date)
    neutral = int(neutral)

    # This is the only place in this function where a failure can happen.
    try:
        result = callback(model_name, sport, away, home, date, neutral, input_timestamp)
    except shared_types.TitanTransientException as err:
        # Logging in this section helps to parse logs.
        full_msg = f"M_ERR_TAG::{model_name}:{type(err).__name__} - {body} - {str(err)}"
        # logging.error(traceback.format_exc())
        logging.error(full_msg)
        notify_titan(body, 0, "failure", titan_config, channel)
        return
    except shared_types.TitanRecurrentException as err:
        result = {"reason": type(err).__name__}
        full_msg = f"M_ERR_TAG::{model_name}:{type(err).__name__} - {body} - {str(err)}"
        # logging.error(traceback.format_exc())
        logging.error(full_msg)
    except Exception as err:  # Includes TitanCriticalExceptions
        logging.error(traceback.format_exception(err))
        logging.error(f"Uncaught exception on {body}")
        notify_titan(body, 0, "critical", titan_config, channel)
        return

    this_game_hash = hash.game_hash(away, home, date)
    output_timestamp = pull_data.update_feature(
        database_resolver(titan_config.sport, titan_config.env),
        model_name,
        this_game_hash,
        input_timestamp,
        result,
        shared_logic.get_secrets(titan_config.secrets_dir),
    )

    if output_timestamp is None:
        full_msg = f"M_ERR_TAG::{model_name}:TYPE_2_TS_ERROR - {body}"
        # logging.error(traceback.format_exc())
        logging.error(full_msg)
        notify_titan(body, 0, "failure", titan_config, channel)
        return

    notify_titan(body, output_timestamp, "success", titan_config, channel)


class RabbitChannel(object):
    def __init__(self, callback: MessageCallback, titan_config: TitanConfig):
        # SSL Context for TLS configuration of Amazon
        def wrapped_callback(ch, method, properties, body):
            logging.info(f"Found {body}")
            process_message(body.decode(), callback, titan_config, self.channel)

        self.callback = wrapped_callback

        self.titan_config = titan_config

        secrets_dir = titan_config.secrets_dir
        rabbitmq_user = shared_logic.get_secrets(secrets_dir)["rabbitmq_user"]
        rabbitmq_password = shared_logic.get_secrets(secrets_dir)["rabbitmq_password"]
        rabbitmq_broker_id = shared_logic.get_secrets(secrets_dir)["rabbitmq_broker_id"]
        url = f"amqps://{rabbitmq_user}:{rabbitmq_password}@{rabbitmq_broker_id}.mq.us-east-2.amazonaws.com:5671"

        # MQ for RabbitMQ
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")
        self.parameters = pika.URLParameters(url)
        self.parameters.ssl_options = pika.SSLOptions(context=ssl_context)
        self.parameters.heartbeat = 600

        self.build_connection()

    def build_connection(self):
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(
            queue=routing_key_resolver(
                self.titan_config.inbound_channel,
                self.titan_config.sport,
                self.titan_config.env,
            )
        )
        if self.titan_config.suffixes:
            for suffix in self.titan_config.suffixes.split(","):
                self.channel.exchange_declare(
                    exchange=self.titan_config.inbound_channel,
                    exchange_type="direct",
                )
                self.channel.queue_bind(
                    exchange=self.titan_config.inbound_channel,
                    queue=routing_key_resolver(
                        self.titan_config.inbound_channel,
                        self.titan_config.sport,
                        self.titan_config.env,
                    ),
                    routing_key=routing_key_resolver(
                        self.titan_config.inbound_channel,
                        self.titan_config.sport,
                        self.titan_config.env,
                        suffix=suffix,
                    ),
                )
        self.channel.queue_declare(
            queue=routing_key_resolver(
                self.titan_config.outbound_channel,
                self.titan_config.sport,
                self.titan_config.env,
            )
        )

    @retrying.retry(wait_fixed=BIGGER_WAIT_SEC * 1000)
    def rebuild_connection(self):
        self.build_connection()


# TODO: Is this the right division of code?
def main(callback: MessageCallback, titan_config: TitanConfig) -> None:
    rc = RabbitChannel(callback, titan_config)

    while True:
        if "prod" == titan_config.env:
            try:
                rc.channel.basic_qos(prefetch_count=PREFETCH_COUNT)
                rc.channel.basic_consume(
                    queue=routing_key_resolver(
                        titan_config.inbound_channel,
                        titan_config.sport,
                        titan_config.env,
                    ),
                    on_message_callback=rc.callback,
                    auto_ack=True,
                )
                rc.channel.start_consuming()
            except:
                logging.error(traceback.format_exc())
                time.sleep(ROLLOVER_WAIT_SEC)
                logging.error("Restarting Pika connection.")
                rc.rebuild_connection()
                # Then try again.
        else:
            # Don't retry
            rc.channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            rc.channel.basic_consume(
                queue=routing_key_resolver(
                    titan_config.inbound_channel, titan_config.sport, titan_config.env
                ),
                on_message_callback=rc.callback,
                auto_ack=True,
            )
            rc.channel.start_consuming()
