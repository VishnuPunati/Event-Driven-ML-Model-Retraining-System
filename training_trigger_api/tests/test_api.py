import json
import pytest

from app.main import app


@pytest.fixture
def client():
    app.testing = True
    with app.test_client() as client:
        yield client


def test_health_check(client):
    response = client.get("/")
    assert response.status_code == 200
    assert b"API is running" in response.data


def test_trigger_retraining_success(client, monkeypatch):
    # Mock the publisher to avoid RabbitMQ dependency
    from app.services import message_publisher

    def mock_publish(self, message):
        return None

    monkeypatch.setattr(
        message_publisher.MessagePublisher,
        "publish",
        mock_publish
    )

    payload = {
        "model_id": "test_model",
        "dataset_version": "v1"
    }

    response = client.post(
        "/trigger-retraining",
        data=json.dumps(payload),
        content_type="application/json"
    )

    assert response.status_code == 202
    assert b"Retraining event accepted" in response.data


def test_trigger_retraining_missing_fields(client):
    payload = {"model_id": "only_model"}

    response = client.post(
        "/trigger-retraining",
        data=json.dumps(payload),
        content_type="application/json"
    )

    assert response.status_code == 400
