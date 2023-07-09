import functools
import logging
import os
import ssl
import time
from typing import Any, Callable, Iterable, Optional, Set, Tuple, Union

import pika
import redis
import retrying
import titanpublic


PREFETCH_COUNT = 100  # Minibatch size
ROLLOVER_WAIT_SEC = 3  # How long to wait before restarting on a Rabbit timeout
BIGGER_WAIT_SEC = 240  # How long to wait if the Rabbit node is down.

RETRIES = 1 if "dev" == os.environ.get("TITAN_ENV", "dev") else None

CallbackSignature = Callable[
    [Optional[str], Optional[str], Optional[str], Optional[str]], None
]
CallbackArgument = Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]
# TODO:Change back
ConditionSignature = Any  # Callable[[], None]


def msg_pad(msg: str) -> CallbackArgument:
    # Dumb shit needed for historical reasons
    return (None, None, None, msg)


class QueueChannel(object):
    def __init__(self):
        self.sport = os.environ.get("SPORT", "ncaam")
        self.env = os.environ.get("TITAN_ENV", "dev")
        self.built = False

        self.retry_exceptions = ()

        self.all_queues: Set[str] = set()
        self.built_queues: Set[str] = set()
        self.build_channel()

    @retrying.retry(
        wait_fixed=BIGGER_WAIT_SEC * 1000,
        stop_max_attempt_number=RETRIES,
    )
    def build_channel(self) -> None:
        logging.error("Establishing queue channnel")
        self._channel = self.build_channel_impl()
        self.built = True

        # Rebuild the queues
        self.built_queues = set()
        for queue_routing_id in self.all_queues:
            self.queue_declare(queue_routing_id)

    def build_channel_impl(self) -> Any:
        raise NotImplementedError

    def queue_declare(self, queue_id: str, suffix: str = "") -> None:
        if not self.built:
            raise AttributeError("Pls build channel first.")
        if not queue_id:
            # Handle a special edge case, so that we don't have to handle outside of
            #  class.
            return
        if queue_id in self.built_queues:
            # This has already been built.
            return

        routing_key = titanpublic.pod_helpers.routing_key_resolver(
            queue_id,
            self.sport,
            self.env,
            suffix=suffix,
        )

        try:
            self.queue_declare_impl(routing_key)
        except self.retry_exceptions:
            # Try rebuilding
            self.build_channel()
            self.queue_declare_impl(routing_key)

        self.all_queues.add(queue_id)
        self.built_queues.add(queue_id)

    def queue_clear(self, queue_id: str, suffix: Optional[str] = None) -> None:
        if suffix:
            raise Exception("suffix isn't supported for this operation")
        if not self.built:
            raise AttributeError("Pls build channel first.")
        if queue_id not in self.all_queues:
            raise Exception(f"Please first build queue {queue_id}")

        routing_key = titanpublic.pod_helpers.routing_key_resolver(
            queue_id,
            self.sport,
            self.env,
        )

        try:
            self.queue_clear_impl(queue_id)
        except self.retry_exceptions:
            self.build_channel()
            self.queue_clear(queue_id)

    def queue_clear_impl(self, routing_key: str) -> None:
        raise NotImplementedError

    def basic_publish(self, msg: str, queue_id: str, suffix: str = "") -> None:
        if not self.built:
            raise AttributeError("Pls build channel first.")
        if queue_id not in self.all_queues:
            raise Exception(f"Please first build queue {queue_id}")

        routing_key = titanpublic.pod_helpers.routing_key_resolver(
            queue_id,
            self.sport,
            self.env,
            suffix=suffix,
        )

        try:
            self.basic_publish_impl(routing_key, msg)
        except self.retry_exceptions:
            self.build_channel()
            self.basic_publish(msg, queue_id, suffix)

    def basic_publish_impl(self, routing_key: str, msg: str) -> None:
        raise NotImplementedError

    def _consume_while_condition(
        self,
        routing_key: str,
        callback: CallbackSignature,
        condition: ConditionSignature,
    ) -> None:
        if not condition():
            return

        while condition():
            for callback_args in self.consumption_impl(routing_key):
                callback(*callback_args)
                if not condition():
                    return

    def consume_while_condition(
        self,
        queue_id: str,
        callback: CallbackSignature,
        condition: ConditionSignature,
        suffix="",
    ) -> None:
        if not self.built:
            raise AttributeError("Pls build channel first.")
        if queue_id not in self.all_queues:
            raise Exception(f"Please first build queue {queue_id}")

        routing_key = titanpublic.pod_helpers.routing_key_resolver(
            queue_id,
            self.sport,
            self.env,
            suffix=suffix,
        )

        try:
            self._consume_while_condition(routing_key, callback, condition)
        except self.retry_exceptions:
            logging.error("Pika exception")
            time.sleep(ROLLOVER_WAIT_SEC)
            self.build_channel()
            self.consume_while_condition(callback, condition)

    def consume_to_death(
        self, queue_id: str, callback: CallbackSignature, suffix: str = ""
    ) -> None:
        self.consume_while_condition(queue_id, callback, lambda: True, suffix=suffix)

    def consumption_impl(self, routing_key: str) -> Iterable[CallbackArgument]:
        raise NotImplementedError


class RedisChannel(QueueChannel):
    def __init__(self, host: str = "localhost", port: int = 6379):
        super().__init__()
        self.r = redis.Redis(host=host, port=port, db=0)

    def build_channel_impl(self) -> None:
        pass  # Nothing to do for redis

    def queue_declare_impl(self, routing_key: str) -> None:
        pass  # Nothing to do for redis

    def queue_clear_impl(self, routing_key: str) -> None:
        self.r.delete(routing_key)

    def basic_publish_impl(self, routing_key: str, msg: str) -> None:
        self.r.rpush(routing_key, msg)

    def consumption_impl(self, routing_key: str) -> Iterable[CallbackArgument]:
        msg = self.r.lpop(routing_key)
        while msg is None:
            msg = self.r.lpop(routing_key)
        yield msg_pad(msg)


class RabbitChannel(QueueChannel):
    def __init__(
        self, rabbitmq_user: str, rabbitmq_password: str, rabbitmq_broker_id: str
    ):
        super().__init__()
        if "prod" == self.env:
            self.retry_exceptions = (*self.retry_exceptions, pika.AMQPError)
        self.rabbitmq_user = rabbitmq_user
        self.rabbitmq_password = rabbitmq_password
        self.rabbitmq_broker_id = rabbitmq_broker_id

    def build_channel_impl(self) -> None:
        logging.error("Starting pika connection")

        # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")

        url = (
            f"amqps://{self.rabbitmq_user}:{self.rabbitmq_password}@"
            f"{self.rabbitmq_broker_id}.mq.us-east-2.amazonaws.com:5671"
        )
        parameters = pika.URLParameters(url)
        parameters.ssl_options = pika.SSLOptions(context=ssl_context)
        parameters.connection_attempts = 3
        parameters.heartbeat = 600

        connection = pika.BlockingConnection(parameters)
        return connection.channel()

    def queue_declare_impl(self, routing_key: str) -> None:
        self._channel.queue_declare(queue=routing_key)

    def basic_publish_impl(self, routing_key: str, msg: str) -> None:
        self._channel.basic_publish(
            exchange="",
            routing_key=routing_key,
            body=msg,
            properties=pika.BasicProperties(delivery_mode=1),
        )

    def consumption_impl(self, routing_key: str) -> Iterable[CallbackArgument]:
        self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
        for method, properties, body in self._channel.consume(
            queue=routing_key,
            auto_ack=True,
            inactivity_timeout=300,
        ):
            if method is None and properties is None and body is None:
                # This is the timeout condition
                logging.debug("Pika timeout")
                raise pika.exceptions.AMQPError()
            yield (None, method, properties, body)


@functools.lru_cache(1)
def get_rabbit_channel(
    rabbitmq_user: str, rabbitmq_password: str, rabbitmq_broker_id: str
) -> RabbitChannel:
    """Creates a singleton"""
    return RabbitChannel(rabbitmq_user, rabbitmq_password, rabbitmq_broker_id)


@functools.lru_cache(1)
def get_redis_channel(host: str = "localhost", port: int = 6379) -> RedisChannel:
    """Creates a singleton"""
    return RedisChannel(host=host, port=port)
