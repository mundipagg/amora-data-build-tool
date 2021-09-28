import json
from hashlib import md5

from google.cloud import bigquery
from google.cloud.bigquery import Table
from amora.compilation import compile

from dbt.models import heart_rate


model = heart_rate.model
source = heart_rate.source()
statement = compile(source)

model_hash = md5(statement.encode()).hexdigest()
temporary_view_id = f"stg-tau-rex.diogo.{model_hash}"


def create_view(query: str) -> Table:
    view = bigquery.Table(temporary_view_id)
    view.view_query = query
    return client.create_table(view, exists_ok=True)


if __name__ == "__main__":
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    view_ref = create_view(str(statement))

    schema = {
        "schema": [
            {
                "name": schema.name,
                "is_nullable": schema.is_nullable,
                "field_type": schema.field_type,
            }
            for schema in view_ref.schema
        ],
        "row_count": view_ref.num_rows,
        "compiled_sql": statement,
    }
    client.delete_table(temporary_view_id)
    print(json.dumps(schema, indent=2))
