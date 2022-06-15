from typing import Union

import pytest
from _pytest.config import ExitCode
from _pytest.main import Session
from dash.testing.composite import DashComposite
from rich.console import Console
from rich.table import Table

from amora.config import settings
from amora.dash.app import dash_app
from amora.tests.audit import AuditLog


def pytest_sessionstart():
    print(
        """
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A amora adoça mais na boca de quem namora. 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Mantido com <3 por `RexData@Stone`
"""
    )


def pytest_sessionfinish(session: Session, exitstatus: Union[int, ExitCode]) -> None:
    log_rows = AuditLog.get_all(test_run_id=settings.TEST_RUN_ID)

    table = Table(
        show_header=True,
        header_style="bold",
        show_lines=True,
        width=settings.CLI_CONSOLE_MAX_WIDTH,
        row_styles=["none", "dim"],
    )
    table.add_column("🧪 Test node id")
    table.add_column("🔎 Bytes billed")
    table.add_column("💰 Estimated cost (USD)")
    for audit_log in log_rows:
        table.add_row(
            audit_log.test_node_id,
            str(audit_log.bytes_billed),
            str(audit_log.estimated_cost_in_usd),
        )

    console = Console(width=settings.CLI_CONSOLE_MAX_WIDTH)
    console.print(table, new_line_start=True)


@pytest.fixture
def amora_dash(dash_duo: DashComposite) -> DashComposite:
    dash_duo.start_server(dash_app)
    return dash_duo
