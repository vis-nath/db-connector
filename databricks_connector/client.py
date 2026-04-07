# Kept for backwards compatibility. New code should use browser_query.query() directly.
from .browser_query import query, DatabricksQueryError

__all__ = ["query", "DatabricksQueryError"]
