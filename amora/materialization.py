from google.cloud.bigquery import Table, Client

client = Client()


def materialize(sql: str, name: str) -> Table:
    view = Table(f"amora-data-build-tool.diogo.{name}")
    view.view_query = sql

    return client.create_table(view, exists_ok=True)
