"""Unit tests with stubs and mocks for Employee Salary average calculation.

Demonstrates using unittest.mock to:
    - Mock the SalaryDataService (external service) that retrieves salary data.
    - Stub the return values so tests do not need real CSV data or Spark.
    - Verify calculate_average() produces correct results with mocked inputs.

Usage:
    uv run pytest src/programming_languages/06_mocks_unit_tests/test_solution.py -v
"""

from unittest.mock import MagicMock, patch

import pytest

from solution import (
    SalaryDataService,
    calculate_average,
)


# ---------------------------------------------------------------------------
# Test 1: calculate_average — pure function tests (no mocks needed)
# ---------------------------------------------------------------------------


class TestCalculateAverage:
    """Tests for the calculate_average() function itself."""

    def test_average_of_integers(self):
        """Average of [100, 200, 300] should be 200.0."""
        result = calculate_average([100.0, 200.0, 300.0])
        assert result == 200.0

    def test_average_of_single_value(self):
        """Average of a single-element list equals that element."""
        result = calculate_average([50000.0])
        assert result == 50000.0

    def test_average_of_salaries(self):
        """Average of realistic salary values."""
        salaries = [79833.0, 260000.0, 85000.0, 20000.0, 150000.0]
        expected = sum(salaries) / len(salaries)
        result = calculate_average(salaries)
        assert result == pytest.approx(expected)

    def test_empty_list_raises_error(self):
        """Empty list should raise ValueError."""
        with pytest.raises(ValueError, match="empty list"):
            calculate_average([])

    def test_average_with_decimals(self):
        """Average with decimal salary values."""
        result = calculate_average([100.50, 200.50])
        assert result == pytest.approx(150.50)


# ---------------------------------------------------------------------------
# Test 2: Mocking SalaryDataService.get_salaries()
# ---------------------------------------------------------------------------


class TestCalculateAverageWithMockedService:
    """Tests using mock to replace the external SalaryDataService."""

    def test_average_with_mocked_service(self):
        """Mock get_salaries() to return a fixed list, verify average."""
        # Create a mock of SalaryDataService
        mock_service = MagicMock(spec=SalaryDataService)

        # Define stub behavior: return fixed salary values
        mock_service.get_salaries.return_value = [
            79833.0, 260000.0, 85000.0, 20000.0, 150000.0
        ]

        # Use the mocked service
        salaries = mock_service.get_salaries()
        result = calculate_average(salaries)

        # Assert
        expected = (79833.0 + 260000.0 + 85000.0 + 20000.0 + 150000.0) / 5
        assert result == pytest.approx(expected)

        # Verify the mock was called
        mock_service.get_salaries.assert_called_once()

    def test_average_with_mocked_experience_filter(self):
        """Mock get_salaries_by_experience() for a specific level."""
        mock_service = MagicMock(spec=SalaryDataService)

        # Stub: senior engineers have these salaries
        mock_service.get_salaries_by_experience.return_value = [
            150000.0, 180000.0, 200000.0, 170000.0
        ]

        salaries = mock_service.get_salaries_by_experience("SE")
        result = calculate_average(salaries)

        expected = (150000.0 + 180000.0 + 200000.0 + 170000.0) / 4
        assert result == pytest.approx(expected)

        # Verify correct argument was passed
        mock_service.get_salaries_by_experience.assert_called_once_with("SE")

    def test_average_with_empty_mocked_response(self):
        """When mock returns empty list, calculate_average raises ValueError."""
        mock_service = MagicMock(spec=SalaryDataService)
        mock_service.get_salaries.return_value = []

        salaries = mock_service.get_salaries()

        with pytest.raises(ValueError):
            calculate_average(salaries)

    def test_mock_service_not_called_unnecessarily(self):
        """Verify that only the expected methods are called."""
        mock_service = MagicMock(spec=SalaryDataService)
        mock_service.get_salaries.return_value = [100000.0, 200000.0]

        salaries = mock_service.get_salaries()
        calculate_average(salaries)

        # get_salaries was called, but get_salaries_by_experience was NOT
        mock_service.get_salaries.assert_called_once()
        mock_service.get_salaries_by_experience.assert_not_called()


# ---------------------------------------------------------------------------
# Test 3: Patching the service at module level (decorator-based mock)
# ---------------------------------------------------------------------------


class TestWithPatchDecorator:
    """Tests using @patch to replace service at the module/class level."""

    @patch.object(SalaryDataService, "get_salaries")
    def test_patched_get_salaries(self, mock_get_salaries):
        """Patch get_salaries method to return controlled data."""
        # Define stub return value
        mock_get_salaries.return_value = [90000.0, 110000.0, 100000.0]

        # Simulate what main() does: call service, then calculate_average
        service = SalaryDataService(spark=MagicMock(), data_path="/fake/path")
        salaries = service.get_salaries()
        result = calculate_average(salaries)

        assert result == pytest.approx(100000.0)
        mock_get_salaries.assert_called_once()

    @patch.object(SalaryDataService, "get_salaries_by_experience")
    def test_patched_experience_filter(self, mock_get_by_exp):
        """Patch experience filter to return entry-level salaries."""
        mock_get_by_exp.return_value = [30000.0, 40000.0, 50000.0]

        service = SalaryDataService(spark=MagicMock(), data_path="/fake/path")
        salaries = service.get_salaries_by_experience("EN")
        result = calculate_average(salaries)

        assert result == pytest.approx(40000.0)
        mock_get_by_exp.assert_called_once_with("EN")

    @patch.object(SalaryDataService, "get_salaries")
    def test_service_returns_large_dataset(self, mock_get_salaries):
        """Simulate a large dataset response from the service."""
        # Stub: 1000 salaries all at $75,000
        mock_get_salaries.return_value = [75000.0] * 1000

        service = SalaryDataService(spark=MagicMock(), data_path="/fake/path")
        salaries = service.get_salaries()
        result = calculate_average(salaries)

        assert result == pytest.approx(75000.0)
        assert len(salaries) == 1000


# ---------------------------------------------------------------------------
# Test 4: Stub fixture (reusable stub via pytest fixture)
# ---------------------------------------------------------------------------


@pytest.fixture
def stub_service():
    """Create a stub SalaryDataService with predefined salary data."""
    service = MagicMock(spec=SalaryDataService)
    service.get_salaries.return_value = [
        60000.0, 80000.0, 100000.0, 120000.0, 140000.0
    ]
    service.get_salaries_by_experience.side_effect = lambda level: {
        "EN": [60000.0, 80000.0],
        "MI": [100000.0],
        "SE": [120000.0, 140000.0],
        "EX": [200000.0, 250000.0, 300000.0],
    }.get(level, [])
    return service


class TestWithStubFixture:
    """Tests using a reusable stub fixture for consistent test data."""

    def test_overall_average(self, stub_service):
        """Calculate average of all salaries from the stub."""
        salaries = stub_service.get_salaries()
        result = calculate_average(salaries)
        assert result == pytest.approx(100000.0)

    def test_entry_level_average(self, stub_service):
        """Entry level average from stub data."""
        salaries = stub_service.get_salaries_by_experience("EN")
        result = calculate_average(salaries)
        assert result == pytest.approx(70000.0)

    def test_senior_level_average(self, stub_service):
        """Senior level average from stub data."""
        salaries = stub_service.get_salaries_by_experience("SE")
        result = calculate_average(salaries)
        assert result == pytest.approx(130000.0)

    def test_executive_level_average(self, stub_service):
        """Executive level average from stub data."""
        salaries = stub_service.get_salaries_by_experience("EX")
        result = calculate_average(salaries)
        assert result == pytest.approx(250000.0)

    def test_unknown_level_raises_error(self, stub_service):
        """Unknown experience level returns empty list -> ValueError."""
        salaries = stub_service.get_salaries_by_experience("XX")
        with pytest.raises(ValueError):
            calculate_average(salaries)
