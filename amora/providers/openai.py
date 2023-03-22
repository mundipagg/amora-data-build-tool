import os

import openai

from amora.ai import SQLPromptAnswer, prompt_context
from amora.logger import log_execution
from amora.sql import format_sql

openai.api_key = os.getenv("OPENAI_API_KEY")


@log_execution()
def sql_translate(question: str, dialect='BigQuery', stop: str = "#", **request_params) -> SQLPromptAnswer:
    """
    SQL generation from natural language questions. Writing queries is hard, giving enough context to AI,
    we can make it easier.

    Args:
        question: Natural language question that is answerable with the project data. E.g: "What is the maximum heart rate observed today?"
        stop: Up to 4 sequences where the API will stop generating further tokens. The returned text will not contain the stop sequence.
            More on: https://platform.openai.com/docs/api-reference/completions/create#completions/create-stop

    Returns:
        The prompt context to be used for completion

    Examples:

    ```python
        assert sql_translate("What is the maximum heart rate observed today?") == SQLPromptAnswer(
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
                "prompt": ...
            },
            sql="SELECT MAX(value) FROM heart_rate WHERE DATE(creationDate) = CURRENT_DATE()"
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
        **request_params
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
