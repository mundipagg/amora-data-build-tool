import typer

from amora.config import settings
from amora.dash.app import dash_app

app = typer.Typer(help="Amora Project documentation")


@app.command("generate")
def generate():
    pass


@app.command("serve")
def serve():
    dash_app.run(debug=True, host=settings.DOCS_HTTP_HOST, port=settings.DOCS_HTTP_PORT)
