from __future__ import annotations

from pathlib import Path

import pytest

from gridlens.spark import get_spark

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    import os

    os.environ.setdefault("GRIDLENS_WAREHOUSE_DIR", str(tmp_path_factory.mktemp("warehouse")))
    session = get_spark("gridlens-tests")
    yield session
    session.stop()
