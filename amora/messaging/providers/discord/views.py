import asyncio

import discord

from amora.messaging.config import settings, logger
from amora.questions import Question


class UpvoteButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.primary,
            emoji='👍',
        )

    async def callback(self, interaction: discord.Interaction):
        self.disabled = True
        logger.info({
            "event": "question:feedback:upvote",
            "interaction_id": interaction.id,
            "user_id": interaction.user.id,
            "user_display_name": interaction.user.display_name
        })


class DownvoteButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.primary,
            emoji='👎',
        )

    async def callback(self, interaction: discord.Interaction):
        self.disabled = True
        logger.info({
            "event": "question:feedback:downvote",
            "interaction_id": interaction.id,
            "user_id": interaction.user.id,
            "user_display_name": interaction.user.display_name
        })


class RefreshButton(discord.ui.Button):
    def __init__(self, question: Question):
        self.question = question
        super().__init__(
            style=discord.ButtonStyle.primary,
            emoji='🔄',
        )

    async def callback(self, interaction: discord.Interaction):
        logger.info({
            "event": "question:refresh:started",
            "interaction_id": interaction.id,
            "user_id": interaction.user.id,
            "user_display_name": interaction.user.display_name
        })

        def get_answer() -> str:
            # Acessamos novamente o data source para trazer uma resposta atualizada
            return self.question.to_markdown()

        # Acesso a rede em código sem interface async.
        answer = await asyncio.get_event_loop().run_in_executor(None, get_answer)

        logger.info({
            "event": "question:refresh:ended",
            "answer": answer,
            "interaction_id": interaction.id,
            "user_id": interaction.user.id,
            "user_display_name": interaction.user.display_name
        })

        await interaction.response.edit_message(content=answer)


class SaveButton(discord.ui.Button):
    def __init__(self, question: Question):
        self.question = question
        super().__init__(
            style=discord.ButtonStyle.primary,
            emoji='💾',
        )

    async def callback(self, interaction: discord.Interaction):
        self.disabled = True

        logger.info({
            "event": "question:save",
            "interaction_id": interaction.id,
            "user_id": interaction.user.id,
            "user_display_name": interaction.user.display_name
        })

        self.question.save(overwrite=True)
        # await interaction.response.send()


def controls_view_for_question(question: Question) -> discord.ui.View:
    """
    Creates a Discord UI view containing buttons for upvoting, downvoting, refreshing and saving a question.

    Parameters:
        question (Question): The question object that the buttons will be associated with.

    Returns:
        discord.ui.View: A Discord UI view object containing the buttons.
    """
    view = discord.ui.View()
    view.add_item(UpvoteButton())
    view.add_item(DownvoteButton())
    view.add_item(RefreshButton(question))
    view.add_item(SaveButton(question))

    return view
