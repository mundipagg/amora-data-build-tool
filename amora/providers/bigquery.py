from google.cloud.bigquery import Client

_client = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = Client()
    return _client
