import logging
import os
import time

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score


class ModelTrainer:
    def __init__(self):
        self.data_path = "/app/data/dummy_dataset.csv"

    def _load_data(self):
        if os.path.exists(self.data_path):
            logging.info("Loading dataset from CSV")
            return pd.read_csv(self.data_path)

        logging.warning("Dataset not found. Generating dummy data")
        np.random.seed(42)

        data = pd.DataFrame({
            "feature_1": np.random.rand(100),
            "feature_2": np.random.rand(100),
            "target": np.random.randint(0, 2, 100)
        })

        return data

    def train(self, model_id: str, dataset_version: str):
        logging.info(
            f"Starting training for model_id={model_id}, dataset_version={dataset_version}"
        )

        data = self._load_data()

        X = data.drop("target", axis=1)
        y = data["target"]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        model = LogisticRegression(max_iter=200)

        # Simulate long-running training
        time.sleep(5)

        model.fit(X_train, y_train)

        predictions = model.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)

        logging.info(
            f"Training completed for model_id={model_id}. Accuracy={accuracy:.4f}"
        )

        return accuracy
