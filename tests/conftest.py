"""
tests/conftest.py

pipelines/ is imported as a package (from pipelines.extract import ...)
because that's how the Airflow DAG imports it. streaming/ files import
each other as flat siblings (from schemas import ...) because inside
their own Docker container they sit directly next to each other, not
as a subpackage. Both patterns are correct for how each actually runs
-- this just makes both resolvable from the repo root when testing.
"""

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "streaming"))
