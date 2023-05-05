import asyncio
from typing import Tuple

import discord

from amora.messaging.config import settings, logger
from amora.messaging.providers.discord.views import controls_view_for_question
from amora.questions import Question


intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)
guild = discord.Object(id=settings.DISCORD_SERVER_ID)
tree = discord.app_commands.CommandTree(client)


async def _get_answer(q: str) -> Tuple[Question, str]:
    def sync_work(prompt: str):
        question = Question.from_prompt(prompt)
        return question, question.to_markdown()

    return await asyncio.get_event_loop().run_in_executor(None, sync_work, q)


@tree.command(
    name="question",
    description="Ask something about the available data",
    guild=guild
)
async def question_command(interaction, prompt: str):
    logger.info({"event": "question:created", "prompt": prompt})

    # renders `[user.name] is thinking...`
    await interaction.response.defer()

    def get_answer(human_prompt: str):
        question = Question.from_prompt(human_prompt)
        return question, question.to_markdown()

    try:
        loop = asyncio.get_event_loop()
        question, answer = await loop.run_in_executor(None, get_answer, prompt)
    except Exception:
        logger.exception({"event": "question:error", "prompt": prompt})
        await interaction.followup.send(content="💥")
        return
    else:
        logger.info({
            "event": "question:answer",
            "prompt": prompt,
            "answer": answer
        })

        await interaction.followup.send(
            content=answer,
            ephemeral=True,
            view=controls_view_for_question(question)
        )


@client.event
async def on_ready():
    logger.info({
        "event": "amora:connected",
        "message": 'Amora has connected to Discord. Syncing local commands with the server...',
        "client_user": client.user
    })

    await tree.sync(guild=guild)

    logger.info({
        "event": "amora:synced",
        "message": 'Commands synced with server.',
        "client_user": client.user
    })
