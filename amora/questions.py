from functools import wraps
from typing import Callable, List

import pandas as pd

from amora.compilation import compile_statement
from amora.providers.bigquery import run
from amora.types import Compilable
from amora.views import View, ViewConfig, ViewKind

QuestionFunc = Callable[[], Compilable]


class Question:
    def __init__(self, question_func: Callable):
        self.__question_func = question_func

    def __call__(self, *args, **kwargs):
        return self.__question_func(*args, **kwargs)

    @property
    def name(self):
        if self.__question_func.__doc__:
            return self.__question_func.__doc__.strip()
        else:
            question = self.__question_func.__name__.replace("_", " ")
            return question.capitalize() + "?"

    @property
    def sql(self):
        stmt = self.__question_func()
        return compile_statement(stmt)

    def answer_df(self) -> pd.DataFrame:
        """
        Executes the question against the target database,
        returning a `pandas.DataFrame` as the answer
        """
        result = run(self.__question_func())
        return result.rows.to_dataframe()

    def render(self) -> View:
        """
        Renders the visual representation of the question's answer
        """
        return View(data=self.answer_df(), config=ViewConfig(kind=ViewKind.table))

    def __str__(self):
        return f"""
## {self.name}

```sql
{self.sql}
```

### Answer

{self.render()}
"""

    def __repr__(self):
        return str(str)


QUESTIONS: List[Question] = []


def question(question_func: Callable):
    @wraps(question_func)
    def wrapper(*args, **kwargs):
        return question_func(*args, **kwargs)

    QUESTIONS.append(Question(question_func))

    return wrapper
