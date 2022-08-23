from typing import Generator, Union

import pytest
from _pytest.config import ExitCode
from _pytest.main import Session
from dash.testing.application_runners import ProcessRunner
from dash.testing.composite import DashComposite
from rich.console import Console
from rich.table import Table

import amora.dash.app
from amora.config import settings
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
def amora_dash(
    request, dash_process_server: ProcessRunner, tmpdir
) -> Generator[DashComposite, None, None]:
    with DashComposite(
        dash_process_server,
        browser=request.config.getoption("webdriver"),
        remote=request.config.getoption("remote"),
        remote_url=request.config.getoption("remote_url"),
        headless=request.config.getoption("headless"),
        options=request.config.hook.pytest_setup_options(),
        download_path=tmpdir.mkdir("download").strpath,
        percy_assets_root=request.config.getoption("percy_assets"),
        percy_finalize=request.config.getoption("nopercyfinalize"),
        pause=request.config.getoption("pause"),
        wait_timeout=60,
    ) as dash_duo:
        dash_duo.start_server(amora.dash.app.__name__, application_name="dash_app")
        yield dash_duo
