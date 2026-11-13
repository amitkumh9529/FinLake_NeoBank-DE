from finlake.utils.logger import get_logger


def test_get_logger_configured():
    logger = get_logger("finlake.test")
    assert logger.name == "finlake.test"
    assert len(logger.handlers) >= 1


def test_spark_session_delta_support(spark, tmp_path):
    delta_path = str(tmp_path / "test_delta_table")
    df = spark.createDataFrame([(1, "ACTIVE"), (2, "CLOSED")], ["id", "status"])
    df.write.format("delta").mode("overwrite").save(delta_path)

    loaded = spark.read.format("delta").load(delta_path)
    assert loaded.count() == 2
