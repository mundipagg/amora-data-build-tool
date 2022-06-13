import typer

app = typer.Typer(help="Amora dashboards")


@app.command("serve")
def serve():
    from amora.dash.app import dash_app
    from amora.dash.config import settings

    dash_app.run(debug=settings.DEBUG, host=settings.HTTP_HOST, port=settings.HTTP_PORT)
