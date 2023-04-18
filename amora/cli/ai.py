import typer

from amora.questions import Question

app = typer.Typer(help="Amora AI")


@app.command("question")
def question(
    human_question: str = typer.Argument(None, help="A natural language question."),
    save: bool = typer.Option(
        False, help="Whether the data question should be saved into a file"
    ),
    overwrite: bool = typer.Option(
        False, help="Whether the data question should overwrite an existing file"
    ),
):
    question = Question.from_prompt(human_question)
    md = question.to_markdown()

    typer.echo(md)
