import json
import logging
import os

from flask import Flask, request, jsonify
from dotenv import load_dotenv

from app.services.message_publisher import MessagePublisher

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

# Initialize publisher (lazy init handled inside class)
publisher = MessagePublisher()


@app.route("/", methods=["GET"])
def health_check():
    return "API is running", 200


@app.route("/trigger-retraining", methods=["POST"])
def trigger_retraining():
    data = request.get_json()

    if not data:
        return jsonify({"error": "Invalid or missing JSON body"}), 400

    model_id = data.get("model_id")
    dataset_version = data.get("dataset_version")

    if not model_id or not dataset_version:
        return jsonify({
            "error": "Both 'model_id' and 'dataset_version' are required"
        }), 400

    event = {
        "model_id": model_id,
        "dataset_version": dataset_version
    }

    try:
        publisher.publish(event)
        logging.info(f"Retraining event published: {event}")
        return jsonify({
            "message": "Retraining event accepted",
            "event": event
        }), 202

    except Exception as e:
        logging.exception("Failed to publish retraining event")
        return jsonify({
            "error": "Failed to enqueue retraining event"
        }), 503


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
