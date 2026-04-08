#!/usr/bin/env python3
"""
Check if the saved Databricks session is still valid.

Exit codes:
  0 — session is valid
  1 — session is expired or missing (run setup_auth.py)

Usage:
  python3 ~/projects/databricks_connector/check_session.py
"""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path.home() / "projects/databricks_connector"))
from databricks_connector import check_session

valid = check_session()

if valid:
    print("Session valid")
    sys.exit(0)
else:
    print("Session expired — run: python3 ~/projects/databricks_connector/setup_auth.py")
    sys.exit(1)
