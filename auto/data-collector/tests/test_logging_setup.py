"""Tests for logging setup module."""
from __future__ import annotations

import logging
import sys

import pytest

from crypto_data_collector.logging_setup import setup_logging


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_setup_logging_sets_level(self):
        """Test that setup_logging sets the correct level."""
        setup_logging("DEBUG")
        root = logging.getLogger()

        assert root.level == logging.DEBUG

    def test_setup_logging_default_level(self):
        """Test that setup_logging defaults to INFO."""
        setup_logging()
        root = logging.getLogger()

        assert root.level == logging.INFO

    def test_setup_logging_case_insensitive(self):
        """Test that level is case insensitive."""
        setup_logging("debug")
        root = logging.getLogger()

        assert root.level == logging.DEBUG

    def test_setup_logging_adds_handler(self):
        """Test that setup_logging adds a stream handler."""
        setup_logging("INFO")
        root = logging.getLogger()

        assert len(root.handlers) >= 1
        assert any(isinstance(h, logging.StreamHandler) for h in root.handlers)

    def test_setup_logging_clears_existing_handlers(self):
        """Test that setup_logging clears existing handlers."""
        root = logging.getLogger()
        # Add some dummy handlers
        root.addHandler(logging.NullHandler())
        root.addHandler(logging.NullHandler())
        initial_count = len(root.handlers)

        setup_logging("INFO")

        # Should have cleared old handlers and added just one new one
        assert len(root.handlers) == 1

    def test_setup_logging_quiets_urllib3(self):
        """Test that urllib3 logger is quieted."""
        setup_logging("DEBUG")
        urllib3_logger = logging.getLogger("urllib3")

        # urllib3 should be at WARNING or higher even when root is DEBUG
        assert urllib3_logger.level >= logging.WARNING

    def test_setup_logging_handler_outputs_to_stdout(self):
        """Test that the handler outputs to stdout."""
        setup_logging("INFO")
        root = logging.getLogger()

        stream_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) >= 1
        assert stream_handlers[0].stream is sys.stdout

    def test_setup_logging_invalid_level_defaults_to_info(self):
        """Test that invalid level defaults to INFO."""
        setup_logging("INVALID_LEVEL")
        root = logging.getLogger()

        assert root.level == logging.INFO

    def test_setup_logging_formatter_includes_timestamp(self):
        """Test that formatter includes timestamp."""
        setup_logging("INFO")
        root = logging.getLogger()

        stream_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        formatter = stream_handlers[0].formatter

        assert formatter is not None
        assert "asctime" in formatter._fmt

    def test_setup_logging_formatter_includes_level(self):
        """Test that formatter includes log level."""
        setup_logging("INFO")
        root = logging.getLogger()

        stream_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        formatter = stream_handlers[0].formatter

        assert formatter is not None
        assert "levelname" in formatter._fmt
