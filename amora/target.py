from dataclasses import dataclass


@dataclass
class Target:
    name: str
    project: str
    type: str = "bigquery"
    max_concurrency: int = 4
    dataset: str = "amora"
