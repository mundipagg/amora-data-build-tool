import typer

from amora.config import settings
from amora.dash.app import dash_app

app = typer.Typer(help="Amora dashboards")


@app.command("generate")
def generate():
    pass


@app.command("serve")
def serve():
    dash_app.run(debug=True, host=settings.DASH_HTTP_HOST, port=settings.DASH_HTTP_PORT)
