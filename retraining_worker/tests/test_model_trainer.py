import time
from worker.model_trainer import ModelTrainer


def test_model_training(monkeypatch):
    trainer = ModelTrainer()

    # Skip actual sleep to make test fast
    monkeypatch.setattr(time, "sleep", lambda x: None)

    accuracy = trainer.train(
        model_id="test_model",
        dataset_version="v1"
    )

    assert accuracy is not None
    assert 0.0 <= accuracy <= 1.0
