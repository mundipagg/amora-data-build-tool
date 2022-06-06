import typer

from amora.dash.app import dash_app
from amora.dash.config import settings

app = typer.Typer(help="Amora dashboards")


@app.command("generate")
def generate():
    pass


@app.command("serve")
def serve():
    dash_app.run(debug=settings.DEBUG, host=settings.HTTP_HOST, port=settings.HTTP_PORT)
