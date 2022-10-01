"""This contains some logic that helps with all the model pods."""

import logging
import os
import ssl
import time
import traceback
from typing import Any, Callable, Dict

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
    env: str = attr.ib()
    secrets_dir: str = attr.ib()
    inbound_channel: str = attr.ib()
    outbound_channel: str = attr.ib()


def notify_titan(
    input_body: str, output_timestamp: int, status: str, outbound_channel: str
) -> None:
    # Defined below
    global channel

    output_body = " ".join(
        [
            input_body,
            str(output_timestamp),
            status,
        ]
    )
    channel.basic_publish(
        exchange="",
        routing_key=outbound_channel,
        body=output_body,
        properties=pika.BasicProperties(delivery_mode=2),
    )


def process_message(
    body: str, callback: MessageCallback, titan_config: TitanConfig
) -> None:
    (
        sport,
        model_name,
        input_timestamp,
        away,
        home,
        date,
        neutral,
    ) = body.split()
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
        notify_titan(body, 0, "failure")
        return
    except shared_types.TitanRecurrentException as err:
        result = {"reason": type(err).__name__}
        full_msg = f"M_ERR_TAG::{model_name}:{type(err).__name__} - {body} - {str(err)}"
        # logging.error(traceback.format_exc())
        logging.error(full_msg)
    except Exception as err:  # Includes TitanCriticalExceptions
        logging.error(traceback.format_exception(err))
        logging.error(f"Uncaught exception on {body}")
        notify_titan(body, 0, "critical")
        return

    this_game_hash = hash.game_hash(away, home, date)
    output_timestamp = pull_data.update_feature(
        sport,
        model_name,
        this_game_hash,
        input_timestamp,
        result,
        shared_logic.get_secrets(os.path.dirname(os.path.abspath(__file__))),
    )
    notify_titan(body, output_timestamp, "success", titan_config.outbound_channel)


class RabbitChannel(object):
    def __init__(self, callback: MessageCallback, titan_config: TitanConfig):
        # SSL Context for TLS configuration of Amazon
        def wrapped_callback(ch, method, properties, body):
            logging.info(f"Found {body}")
            process_message(body.decode(), callback, titan_config)

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
        self.channel.queue_declare(queue=self.titan_config.inbound_channel)
        self.channel.queue_declare(queue=self.titan_config.outbound_channel)

    @retrying.retry(wait_fixed=BIGGER_WAIT_SEC * 1000)
    def rebuild_connection(self):
        self.build_connection()


def main(callback: MessageCallback, titan_config: TitanConfig) -> None:
    channel = RabbitChannel(callback, titan_config)

    while True:
        if "prod" == titan_config.env:
            try:
                # TODO: Is this the right division?
                channel.basic_qos(prefetch_count=PREFETCH_COUNT)
                channel.basic_consume(
                    queue=titan_config.outbound_channel,
                    on_message_callback=channel.callback,
                    auto_ack=True,
                )
                channel.start_consuming()
            except:
                logging.error(traceback.format_exc())
                time.sleep(ROLLOVER_WAIT_SEC)
                logging.error("Restarting Pika connection.")
                channel.rebuild_connection()
                # Then try again.
        else:
            # Don't retry
            channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            channel.basic_consume(
                queue=titan_config.env,
                on_message_callback=channel.callback,
                auto_ack=True,
            )
            channel.start_consuming()


def routing_key_resolver(id: str, sport: str, env: str, suffix: str = "") -> str:
    dev_suffix = ""
    if "dev" == env:
        dev_suffix = "-dev"

    if suffix:
        suffix = f"-{suffix}"

    return f"{id}-{sport}{dev_suffix}{suffix}"
