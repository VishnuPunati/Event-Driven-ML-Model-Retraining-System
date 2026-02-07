import json
from unittest.mock import MagicMock, patch

from app.services.message_publisher import MessagePublisher


@patch("app.services.message_publisher.pika.BlockingConnection")
def test_publish_message(mock_connection):
    # Mock channel
    mock_channel = MagicMock()
    mock_connection.return_value.channel.return_value = mock_channel

    publisher = MessagePublisher()

    message = {
        "model_id": "test_model",
        "dataset_version": "v1"
    }

    publisher.publish(message)

    # Ensure queue declared as durable
    mock_channel.queue_declare.assert_called_with(
        queue="retraining_queue",
        durable=True
    )

    # Ensure message published
    mock_channel.basic_publish.assert_called_once()

    args, kwargs = mock_channel.basic_publish.call_args

    assert json.loads(kwargs["body"]) == message
    assert kwargs["properties"].delivery_mode == 2
