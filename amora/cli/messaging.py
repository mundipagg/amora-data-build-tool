import typer

from amora.messaging.providers.discord.client import client
from amora.messaging.config import settings


app = typer.Typer(help="Messaging integration")

discord = typer.Typer(help="Discord providers. Requires `AMORA_MESSAGING_` configuration")
app.add_typer(discord, name="discord")


@discord.command("serve")
def discord_serve():
    client.run(settings.DISCORD_TOKEN)

