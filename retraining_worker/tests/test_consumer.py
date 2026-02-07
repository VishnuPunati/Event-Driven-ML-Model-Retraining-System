import json
from unittest.mock import MagicMock, patch

from worker.consumer import RetrainingConsumer


@patch("worker.consumer.pika.BlockingConnection")
@patch("worker.consumer.ModelTrainer")
def test_consumer_acknowledges_message(mock_trainer, mock_connection):
    # Mock channel
    mock_channel = MagicMock()
    mock_connection.return_value.channel.return_value = mock_channel

    consumer = RetrainingConsumer()

    # Prepare fake message
    body = json.dumps({
        "model_id": "test_model",
        "dataset_version": "v1"
    }).encode()

    method = MagicMock()
    method.delivery_tag = "test-tag"

    # Simulate callback execution
    consumer._callback(
        ch=mock_channel,
        method=method,
        properties=None,
        body=body
    )

    # Trainer should be called
    consumer.trainer.train.assert_called_once()

    # Message should be ACKed
    mock_channel.basic_ack.assert_called_with(
        delivery_tag="test-tag"
    )
