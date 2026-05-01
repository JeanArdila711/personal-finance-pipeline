"""Tests para pipeline.py. Mockean extract, load y dbt pa' no llamar APIs reales."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.pipeline import main, run_dbt


def test_pipeline_calls_extract_then_load_then_dbt(tmp_path):
    """El pipeline corre extract → load → dbt en ese orden."""
    with (
        patch("src.pipeline.extract_main") as mock_extract,
        patch("src.pipeline.load_main") as mock_load,
        patch("src.pipeline.run_dbt") as mock_dbt,
    ):
        mock_extract.return_value = 0
        mock_load.return_value = 0
        mock_dbt.return_value = 0

        result = main(["--days", "7", "--db", str(tmp_path / "test.db")])

        assert result == 0
        mock_extract.assert_called_once()
        mock_load.assert_called_once()
        mock_dbt.assert_called_once()


def test_pipeline_stops_if_extract_fails(tmp_path):
    """Si extract falla, load y dbt NO se corren."""
    with (
        patch("src.pipeline.extract_main") as mock_extract,
        patch("src.pipeline.load_main") as mock_load,
        patch("src.pipeline.run_dbt") as mock_dbt,
    ):
        mock_extract.return_value = 1

        result = main(["--days", "7", "--db", str(tmp_path / "test.db")])

        assert result == 1
        mock_load.assert_not_called()
        mock_dbt.assert_not_called()


def test_pipeline_stops_if_load_fails(tmp_path):
    """Si load falla, dbt NO se corre."""
    with (
        patch("src.pipeline.extract_main") as mock_extract,
        patch("src.pipeline.load_main") as mock_load,
        patch("src.pipeline.run_dbt") as mock_dbt,
    ):
        mock_extract.return_value = 0
        mock_load.return_value = 1

        result = main(["--days", "7", "--db", str(tmp_path / "test.db")])

        assert result == 1
        mock_dbt.assert_not_called()


def test_pipeline_skip_dbt_flag(tmp_path):
    """--skip-dbt hace que dbt NO se corra."""
    with (
        patch("src.pipeline.extract_main") as mock_extract,
        patch("src.pipeline.load_main") as mock_load,
        patch("src.pipeline.run_dbt") as mock_dbt,
    ):
        mock_extract.return_value = 0
        mock_load.return_value = 0

        result = main(["--days", "7", "--db", str(tmp_path / "test.db"), "--skip-dbt"])

        assert result == 0
        mock_dbt.assert_not_called()


def test_run_dbt_returns_nonzero_on_failure():
    """run_dbt retorna exit code no-cero si dbt falla."""
    with (
        patch("src.pipeline.subprocess.run") as mock_run,
        patch("src.pipeline.Path.exists", return_value=True),
    ):
        mock_run.return_value = MagicMock(returncode=1)

        result = run_dbt(dbt_dir="dbt", profiles_dir="dbt")

        assert result == 1


def test_run_dbt_returns_zero_on_success():
    """run_dbt retorna 0 si dbt run y dbt test pasan."""
    with (
        patch("src.pipeline.subprocess.run") as mock_run,
        patch("src.pipeline.Path.exists", return_value=True),
    ):
        mock_run.return_value = MagicMock(returncode=0)

        result = run_dbt(dbt_dir="dbt", profiles_dir="dbt")

        assert result == 0
