# Kept for backwards compatibility. New code should use query.query() directly.
from .query import query, DatabricksQueryError

__all__ = ["query", "DatabricksQueryError"]
