"""Abstract base for format readers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class BaseFormatReader(ABC):
    """Protocol for reading a specific file format into Python objects.

    Each reader normalises a file (or raw bytes) into a common representation:
    a list of dictionaries (one per row/record).
    """

    format_name: str = "base"

    @abstractmethod
    def read(self, source: str | Path | bytes) -> list[dict[str, Any]]:
        """Read *source* and return a list of record dictionaries.

        Args:
            source: File path or raw bytes.
        """
