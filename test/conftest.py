import pytest as pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    session = SparkSession.builder.getOrCreate()
    yield session
    session.stop()
