import webbrowser

from databricks.sdk import WorkspaceClient

_HOST = "https://dbc-6f0786a7-8ba5.cloud.databricks.com"
_client: WorkspaceClient | None = None


def _patch_browser_print_url() -> None:
    """Print the OAuth URL instead of opening a browser.

    The user pastes it into their Windows browser. WSL2 forwards
    localhost:8020 back to this process automatically.
    """
    def _open(url, new=0, autoraise=True):
        print("\n" + "=" * 60)
        print("Open this URL in your browser to log in:")
        print(url)
        print("=" * 60 + "\n")
        return True

    webbrowser.open = _open
    webbrowser.open_new = _open
    webbrowser.open_new_tab = _open


def get_client() -> WorkspaceClient:
    """
    Return a singleton WorkspaceClient authenticated via U2M OAuth.

    First call prints a URL — open it in your Windows browser and log in
    with your @kavak.com account. WSL2 forwards the localhost:8020 callback
    back to this process. Subsequent calls return the cached instance silently.
    """
    global _client
    if _client is None:
        _patch_browser_print_url()
        _client = WorkspaceClient(host=_HOST, auth_type="external-browser")
    return _client
