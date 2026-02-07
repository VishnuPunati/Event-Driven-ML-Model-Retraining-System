import json
import logging
import os
import time

import pika

from worker.model_trainer import ModelTrainer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


class RetrainingConsumer:
    def __init__(self):
        self.queue_name = "retraining_queue"
        self.connection = None
        self.channel = None
        self.trainer = ModelTrainer()
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

                # Process only one message at a time
                self.channel.basic_qos(prefetch_count=1)

                logging.info("Worker connected to RabbitMQ")
                return

            except pika.exceptions.AMQPConnectionError:
                logging.warning(
                    f"RabbitMQ connection failed (attempt {attempt}/{retries}). Retrying..."
                )
                time.sleep(delay)

        raise ConnectionError("Worker could not connect to RabbitMQ")

    def _callback(self, ch, method, properties, body):
        try:
            message = json.loads(body)
            logging.info(f"Received message: {message}")

            self.trainer.train(
                model_id=message.get("model_id"),
                dataset_version=message.get("dataset_version")
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.info("Message processed successfully")

        except Exception as e:
            logging.exception("Error processing message")
            ch.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False
            )

    def start(self):
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self._callback
        )

        logging.info("Waiting for retraining jobs...")
        self.channel.start_consuming()


if __name__ == "__main__":
    consumer = RetrainingConsumer()
    consumer.start()
