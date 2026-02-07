import json
import logging
import os
import time

import pika


class MessagePublisher:
    def __init__(self):
        self.queue_name = "retraining_queue"
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self, retries=5, delay=5):
        credentials = pika.PlainCredentials(
            os.getenv("RABBITMQ_USER"),
            os.getenv("RABBITMQ_PASS")
        )

        parameters = pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST"),
            port=int(os.getenv("RABBITMQ_PORT")),
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )

        for attempt in range(1, retries + 1):
            try:
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()

                self.channel.queue_declare(
                    queue=self.queue_name,
                    durable=True
                )

                logging.info("Connected to RabbitMQ successfully")
                return

            except pika.exceptions.AMQPConnectionError as e:
                logging.warning(
                    f"RabbitMQ connection failed (attempt {attempt}/{retries}). Retrying..."
                )
                time.sleep(delay)

        raise ConnectionError("Could not connect to RabbitMQ after retries")

    def publish(self, message: dict):
        if not self.channel or self.channel.is_closed:
            logging.warning("RabbitMQ channel closed. Reconnecting...")
            self._connect()

        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2  # Make message persistent
            )
        )

        logging.info(f"Message published to queue: {message}")

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logging.info("RabbitMQ connection closed")
