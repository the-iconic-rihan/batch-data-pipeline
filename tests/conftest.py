from __future__ import annotations

import os
import shutil

import pytest

from pipeline.config import Settings


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(data_dir=tmp_path, rows_per_day=2_000, max_invalid_ratio=0.05)


@pytest.fixture(scope="session")
def spark():
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        pytest.skip("Java not installed - Spark tests run in CI")
    from pipeline.transform import build_spark

    session = build_spark("tests")
    yield session
    session.stop()
