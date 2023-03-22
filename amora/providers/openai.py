import os

import openai
from pydantic import BaseModel

from amora.ai import prompt_context
from amora.config import settings
from amora.logger import log_execution
from amora.sql import format_sql

openai.api_key = os.getenv("OPENAI_API_KEY")


class SQLPromptAnswer(BaseModel):
    """
    Represents the response of a SQL prompt completion, the generated SQL code, cost, and other metadata related
    to the completion request.
    """

    sql: str
    """A string containing the completed SQL code."""

    completion_tokens: int
    """An integer representing the number of tokens added to complete the SQL prompt."""
    prompt_tokens: int
    """An integer representing the number of tokens in the prompt."""
    total_tokens: int
    """An integer representing the total number of tokens in the completed SQL code"""

    request_params: dict
    """A dictionary containing the parameters used for the completion request."""
    response_ms: int
    """An integer representing the response time in milliseconds."""

    class Config:
        arbitrary_types_allowed = True

    @property
    def estimated_cost_in_usd(self):
        """
        The estimated cost of completion. OpenAI has a per 1000 tokens pricing. You can think of tokens as pieces of words, where 1,000 tokens is
        about 750 words.

        Read More:
            https://openai.com/pricing
        """
        return (self.total_tokens / 1000) * settings.OPENAI_COST_PER_1000_TOKENS


@log_execution()
def sql_translate(
    question: str, dialect: str = "BigQuery", stop: str = "#", **request_params
) -> SQLPromptAnswer:
    """
    Translates natural language questions to SQL. By providing sufficient context to artificial intelligence, we can
    make writing SQL queries easier by leveraging it's ability to translate natural language questions into SQL code.

    Warnings:
        Despite using a highly capable state-of-the-art model, the solution design must take into consideration
        that A.I. isn't able to give only right answers. It's recommended to [design with humans in the loop](https://hai.stanford.edu/news/humans-loop-design-interactive-ai-systems),
        in order to fact-check the output, specialy in critical domains.

    Args:
        question: Natural language question that is answerable with the project data. E.g: "What is the maximum heart rate observed today?"
        dialect: The SQL dialect to be used.
        stop: Up to 4 sequences where the API will stop generating further tokens. The returned text will not contain the stop sequence.
            More on: https://platform.openai.com/docs/api-reference/completions/create#completions/create-stop

    Returns:
        The generated SQL code, cost, and other metadata related to the completion request.

    Read more:
        [https://platform.openai.com/examples/default-sql-translate](https://platform.openai.com/examples/default-sql-translate)

    Examples:

        ```python
        assert sql_translate(
            "What is the maximum heart rate observed today?"
        ) == SQLPromptAnswer(
            completion_tokens=100,
            prompt_tokens=35,
            total_tokens=135,
            request_params={
                "model": "code-davinci-002",
                "temperature": 0,
                "max_tokens": 150,
                "top_p": 1.0,
                "frequency_penalty": 0.0,
                "presence_penalty": 0.0,
                "stop": ["#"],
                "prompt": ...,
            },
            sql="SELECT MAX(value) FROM heart_rate WHERE DATE(creationDate) = CURRENT_DATE()",
        )
        ```

    """
    request_params = dict(
        model="code-davinci-002",
        temperature=0,
        max_tokens=150,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0,
        stop=[stop],
        prompt=f""" 
            ### {dialect} SQL tables, with their properties:
            {prompt_context(stop)}
            ### A query to answer '{question}'
            SELECT
        """,
        **request_params,
    )
    completion = openai.Completion.create(**request_params)
    assert len(completion["choices"]) == 1

    return SQLPromptAnswer(
        response_ms=completion.response_ms,
        completion_tokens=completion.usage.completion_tokens,
        prompt_tokens=completion.usage.prompt_tokens,
        total_tokens=completion.usage.total_tokens,
        sql=format_sql(f"SELECT {completion.choices[0].text}"),
        request_params=request_params,
    )
