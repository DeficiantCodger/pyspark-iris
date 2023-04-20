"""
This is a boilerplate pipeline
generated using Kedro 0.18.6
"""
import os
import sys
import logging
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
os.environ['HADOOP_HOME'] = "C:/hadoop-2.9.2"
sys.path.append("C:/hadoop-2.9.2/bin")

def split_data(data: DataFrame, parameters: Dict) -> Tuple:
    """Splits data into features and targets training and test sets.

    Args:
        data: Data containing features and target.
        parameters: Parameters defined in parameters.yml.
    Returns:
        Split data.
    """
    
    spark = SparkSession.builder.appName("Databricks Shell").getOrCreate()
    spark.sql(''' select 8 ''').show()
    data.show()

    # Split to training and testing data
    data_train, data_test = data.randomSplit(
        weights=[parameters["train_fraction"], 1 - parameters["train_fraction"]]
    )

    X_train = data_train.drop(parameters["target_column"])
    X_test = data_test.drop(parameters["target_column"])
    y_train = data_train.select(parameters["target_column"])
    y_test = data_test.select(parameters["target_column"])

    return X_train, X_test, y_train, y_test


def make_predictions(
    X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.DataFrame
) -> DataFrame:
    """Uses 1-nearest neighbour classifier to create predictions.

    Args:
        X_train: Training data of features.
        y_train: Training data for target.
        X_test: Test data for features.

    Returns:
        y_pred: Prediction of the target variable.
    """

    X_train_numpy = X_train.to_numpy()
    X_test_numpy = X_test.to_numpy()

    squared_distances = np.sum(
        (X_train_numpy[:, None, :] - X_test_numpy[None, :, :]) ** 2, axis=-1
    )
    nearest_neighbour = squared_distances.argmin(axis=0)
    y_pred = y_train.iloc[nearest_neighbour]
    y_pred.index = X_test.index

    return y_pred


def report_accuracy(y_pred: pd.Series, y_test: pd.Series):
    """Calculates and logs the accuracy.

    Args:
        y_pred: Predicted target.
        y_test: True target.
    """
    accuracy = (y_pred == y_test).sum() / len(y_test)
    logger = logging.getLogger(__name__)
    logger.info("Model has an accuracy of %.3f on test data.", accuracy)
