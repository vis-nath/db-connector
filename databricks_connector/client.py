from databricks.sdk import WorkspaceClient

_HOST = "https://dbc-6f0786a7-8ba5.cloud.databricks.com"
_client: WorkspaceClient | None = None


def get_client() -> WorkspaceClient:
    """
    Return a singleton WorkspaceClient authenticated via U2M OAuth.

    First call opens a browser window for login.
    Subsequent calls return the cached instance silently.
    Token refresh is handled automatically by the SDK.
    """
    global _client
    if _client is None:
        _client = WorkspaceClient(host=_HOST)
    return _client
