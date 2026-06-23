"""Unit tests for IBM AML Transactions data processing (pytest).

Tests verify:
    1. Correct number of columns retrieved from the dataset.
    2. Amount Received column contains float (double) values.
    3. All values in Timestamp column have datetime format (yyyy/MM/dd HH:mm).
    4. Reinvestment is a valid payment format.

Usage:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run pytest src/programming_languages/05_unit_tests/test_solution.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from solution import (
    EXPECTED_COLUMNS,
    TIMESTAMP_FORMAT,
    VALID_PAYMENT_FORMATS,
    get_column_count,
    get_invalid_timestamps,
    is_valid_payment_format,
    load_transactions,
    validate_amount_received_is_float,
    validate_timestamp_format,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

DATA_FILE = "data/input/HI-Large_Trans.csv"


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession shared across all tests in the session."""
    session = (
        SparkSession.builder.appName("UnitTests_pytest")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def transactions_df(spark):
    """Load the transactions DataFrame once for all tests."""
    from pathlib import Path

    path = Path(__file__).resolve().parent / DATA_FILE
    assert path.exists(), f"Data file not found: {path}. Run download_data.py first."
    return load_transactions(spark, str(path))


# ---------------------------------------------------------------------------
# Test 1: Correct number of columns
# ---------------------------------------------------------------------------


class TestColumnCount:
    """Tests for verifying the correct number of columns in the dataset."""

    def test_column_count_equals_expected(self, transactions_df):
        """The dataset should have exactly 11 columns."""
        assert get_column_count(transactions_df) == EXPECTED_COLUMNS

    def test_column_count_is_positive(self, transactions_df):
        """Column count must be a positive integer."""
        count = get_column_count(transactions_df)
        assert count > 0

    def test_expected_columns_present(self, transactions_df):
        """Key columns must be present in the DataFrame."""
        expected = [
            "Timestamp",
            "From Bank",
            "Amount Received",
            "Payment Format",
            "Is Laundering",
        ]
        for col_name in expected:
            assert col_name in transactions_df.columns, f"Missing column: {col_name}"


# ---------------------------------------------------------------------------
# Test 2: Amount Received contains float values
# ---------------------------------------------------------------------------


class TestAmountReceived:
    """Tests for validating that Amount Received contains float values."""

    def test_amount_received_is_double_type(self, transactions_df):
        """Amount Received column should be DoubleType (float)."""
        assert validate_amount_received_is_float(transactions_df) is True

    def test_amount_received_schema_type(self, transactions_df):
        """Verify via schema inspection that the type is DoubleType."""
        field = transactions_df.schema["Amount Received"]
        assert isinstance(field.dataType, DoubleType)

    def test_amount_received_no_negative(self, transactions_df):
        """Amount Received values should be non-negative (financial amounts)."""
        negative_count = transactions_df.filter(
            F.col("Amount Received") < 0
        ).count()
        assert negative_count == 0, f"Found {negative_count} negative amounts"

    def test_amount_received_has_values(self, transactions_df):
        """Amount Received should have non-null values."""
        non_null = transactions_df.filter(
            F.col("Amount Received").isNotNull()
        ).count()
        assert non_null > 0


# ---------------------------------------------------------------------------
# Test 3: Timestamp column has datetime format
# ---------------------------------------------------------------------------


class TestTimestampFormat:
    """Tests for validating datetime format in the Timestamp column."""

    def test_no_invalid_timestamps(self, transactions_df):
        """All non-null Timestamp values should parse as datetime."""
        invalid_count = get_invalid_timestamps(transactions_df)
        assert invalid_count == 0, f"Found {invalid_count} invalid timestamps"

    def test_timestamp_parses_correctly(self, spark):
        """Verify parsing with a known timestamp value."""
        test_df = spark.createDataFrame(
            [("2022/08/01 00:17",)], ["Timestamp"]
        )
        parsed = validate_timestamp_format(test_df)
        result = parsed.select("parsed_ts").first()[0]
        assert result is not None
        assert result.year == 2022
        assert result.month == 8
        assert result.day == 1

    def test_invalid_timestamp_detected(self, spark):
        """Timestamps not matching the format should result in null."""
        test_df = spark.createDataFrame(
            [("not-a-date",), ("2022-13-45 99:99",)], ["Timestamp"]
        )
        parsed = validate_timestamp_format(test_df)
        null_count = parsed.filter(F.col("parsed_ts").isNull()).count()
        assert null_count == 2

    def test_timestamp_column_not_empty(self, transactions_df):
        """Timestamp column should have non-null values."""
        non_null = transactions_df.filter(
            F.col("Timestamp").isNotNull()
        ).count()
        assert non_null > 0


# ---------------------------------------------------------------------------
# Test 4: Reinvestment is a valid payment format
# ---------------------------------------------------------------------------


class TestPaymentFormat:
    """Tests for validating Reinvestment as a valid payment format."""

    def test_reinvestment_is_valid(self):
        """Reinvestment must be in the list of valid payment formats."""
        assert is_valid_payment_format("Reinvestment") is True

    def test_reinvestment_exists_in_data(self, transactions_df):
        """Reinvestment should actually appear in the dataset."""
        reinvestment_count = transactions_df.filter(
            F.col("Payment Format") == "Reinvestment"
        ).count()
        assert reinvestment_count > 0, "Reinvestment not found in dataset"

    def test_all_payment_formats_valid(self, transactions_df):
        """Every Payment Format in the data should be in the valid list."""
        formats_in_data = (
            transactions_df.select("Payment Format")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        for fmt in formats_in_data:
            if fmt is not None:
                assert fmt in VALID_PAYMENT_FORMATS, f"Invalid format: {fmt}"

    def test_invalid_format_rejected(self):
        """An unknown format should not be considered valid."""
        assert is_valid_payment_format("FakeFormat") is False
        assert is_valid_payment_format("") is False

    def test_all_known_formats_valid(self):
        """Every format in VALID_PAYMENT_FORMATS should pass validation."""
        for fmt in VALID_PAYMENT_FORMATS:
            assert is_valid_payment_format(fmt) is True
