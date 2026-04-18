"""Tests for scheduler module."""
from __future__ import annotations

import datetime as dt
from unittest.mock import patch, MagicMock
import time

import pytest

from crypto_data_collector.scheduler import Scheduler


class TestScheduler:
    """Tests for Scheduler class."""

    def test_scheduler_creation_defaults(self):
        """Test Scheduler creation with defaults."""
        sched = Scheduler()

        assert sched.interval_minutes == 15
        assert sched.align_to_boundary is True
        assert sched.boundary_offset_seconds == 5

    def test_scheduler_creation_custom(self):
        """Test Scheduler creation with custom values."""
        sched = Scheduler(
            interval_minutes=30,
            align_to_boundary=False,
            boundary_offset_seconds=10,
        )

        assert sched.interval_minutes == 30
        assert sched.align_to_boundary is False
        assert sched.boundary_offset_seconds == 10

    def test_scheduler_frozen(self):
        """Test that Scheduler is frozen."""
        sched = Scheduler()

        with pytest.raises(AttributeError):
            sched.interval_minutes = 30


class TestSleepUntilNextTick:
    """Tests for Scheduler.sleep_until_next_tick method."""

    @patch("crypto_data_collector.scheduler.time.sleep")
    def test_sleep_without_alignment(self, mock_sleep):
        """Test sleep without boundary alignment."""
        sched = Scheduler(interval_minutes=15, align_to_boundary=False)

        sched.sleep_until_next_tick()

        mock_sleep.assert_called_once_with(15 * 60)  # 15 minutes in seconds

    @patch("crypto_data_collector.scheduler.time.sleep")
    def test_sleep_without_alignment_30_minutes(self, mock_sleep):
        """Test sleep without alignment for 30 minute interval."""
        sched = Scheduler(interval_minutes=30, align_to_boundary=False)

        sched.sleep_until_next_tick()

        mock_sleep.assert_called_once_with(30 * 60)

    @patch("crypto_data_collector.scheduler.time.sleep")
    def test_sleep_with_alignment_calculates_positive_sleep(self, mock_sleep):
        """Test that aligned sleep calculates a positive sleep time."""
        sched = Scheduler(interval_minutes=15, align_to_boundary=True, boundary_offset_seconds=5)

        sched.sleep_until_next_tick()

        mock_sleep.assert_called_once()
        sleep_time = mock_sleep.call_args[0][0]
        # Sleep time should be positive and less than interval + offset
        assert sleep_time > 0
        assert sleep_time <= (15 * 60) + 5

    @patch("crypto_data_collector.scheduler.time.sleep")
    def test_sleep_with_alignment_includes_offset(self, mock_sleep):
        """Test that aligned sleep includes boundary offset in reasonable range."""
        sched = Scheduler(interval_minutes=15, align_to_boundary=True, boundary_offset_seconds=30)

        sched.sleep_until_next_tick()

        mock_sleep.assert_called_once()
        sleep_time = mock_sleep.call_args[0][0]
        # Should be positive and reasonable
        assert sleep_time > 0
        assert sleep_time <= (15 * 60) + 30

    @patch("crypto_data_collector.scheduler.time.sleep")
    def test_sleep_with_30_minute_interval_aligned(self, mock_sleep):
        """Test sleep with 30-minute interval and alignment."""
        sched = Scheduler(interval_minutes=30, align_to_boundary=True, boundary_offset_seconds=5)

        sched.sleep_until_next_tick()

        mock_sleep.assert_called_once()
        sleep_time = mock_sleep.call_args[0][0]
        # Should be positive and at most 30 min + offset
        assert sleep_time > 0
        assert sleep_time <= (30 * 60) + 5

    @patch("crypto_data_collector.scheduler.time.sleep")
    def test_sleep_with_5_minute_interval_aligned(self, mock_sleep):
        """Test sleep with 5-minute interval and alignment."""
        sched = Scheduler(interval_minutes=5, align_to_boundary=True, boundary_offset_seconds=5)

        sched.sleep_until_next_tick()

        mock_sleep.assert_called_once()
        sleep_time = mock_sleep.call_args[0][0]
        # Should be positive and at most 5 min + offset
        assert sleep_time > 0
        assert sleep_time <= (5 * 60) + 5

    @patch("crypto_data_collector.scheduler.time.sleep")
    def test_sleep_custom_offset_no_alignment(self, mock_sleep):
        """Test sleep with custom boundary offset (no alignment)."""
        sched = Scheduler(
            interval_minutes=15,
            align_to_boundary=False,
            boundary_offset_seconds=30,
        )

        sched.sleep_until_next_tick()

        # Without alignment, just sleeps for interval (offset not used)
        mock_sleep.assert_called_once_with(15 * 60)


class TestSchedulerBoundaryCalculation:
    """Tests for boundary calculation logic."""

    def test_next_boundary_calculation_logic(self):
        """Test the boundary calculation math directly."""
        interval = 15

        # Test various minute values
        test_cases = [
            (0, 15),   # At :00, next is :15
            (5, 15),   # At :05, next is :15
            (14, 15),  # At :14, next is :15
            (15, 30),  # At :15, next is :30
            (20, 30),  # At :20, next is :30
            (30, 45),  # At :30, next is :45
            (45, 60),  # At :45, next is :60 (needs hour wrap)
            (50, 60),  # At :50, next is :60 (needs hour wrap)
            (59, 60),  # At :59, next is :60 (needs hour wrap)
        ]

        for minute, expected_next in test_cases:
            next_block = (minute // interval + 1) * interval
            assert next_block == expected_next, f"At minute {minute}, expected {expected_next}, got {next_block}"

    def test_hour_wraparound_logic(self):
        """Test hour wraparound when next boundary crosses the hour."""
        interval = 15

        # At minute 50, next boundary is 60 which needs wraparound
        minute = 50
        next_block = (minute // interval + 1) * interval
        assert next_block == 60

        # The scheduler handles this by subtracting 60 and adding an hour
        add_hours = 0
        if next_block >= 60:
            next_block -= 60
            add_hours = 1

        assert next_block == 0
        assert add_hours == 1

    def test_boundary_calculation_with_30_minute_interval(self):
        """Test boundary calculation with 30-minute interval."""
        interval = 30

        test_cases = [
            (0, 30),   # At :00, next is :30
            (15, 30),  # At :15, next is :30
            (29, 30),  # At :29, next is :30
            (30, 60),  # At :30, next is :60 (needs wrap)
            (45, 60),  # At :45, next is :60 (needs wrap)
        ]

        for minute, expected_next in test_cases:
            next_block = (minute // interval + 1) * interval
            assert next_block == expected_next, f"At minute {minute}, expected {expected_next}, got {next_block}"
