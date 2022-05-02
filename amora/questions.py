from functools import wraps
from typing import Callable, List

import pandas as pd

from amora.compilation import compile_statement
from amora.providers.bigquery import run
from amora.types import Compilable
from amora.views import View, ViewConfig, ViewKind

QuestionFunc = Callable[[], Compilable]


class Question:
    """
    A Data Question is a __query__, its __results__, and its __visualization__.

    The __query__ is expressed as an `amora.types.Compilable`, its __results__ is a
    `pandas.DataFrame` and the results __visualization__ as an `amora.views.View`.

    ## Example

    Lets define a new data question:

    ```python
    from amora.questions import question
    from examples.models.step_count_by_source import StepCountBySource


    @question
    def what_are_the_available_data_sources():
        return select(StepCountBySource.source_name).distinct()
    ```

    ### Query
    The query is accessible through the `sql` property:

    ```python
    what_are_the_available_data_sources.sql
    ```

    ```sql
    SELECT DISTINCT `step_count_by_source`.`source_name`
    FROM `amora-data-build-tool.amora`.`step_count_by_source`
    ```

    ### Results

    ```python
    what_are_the_available_data_sources.answer_df()
    ```

    ```
        source_name
    0  Diogo iPhone
    1        Mi Fit
    2        iPhone
    ```

    ### Visualization


    ```python
    what_are_the_available_data_sources.render()
    ```

    |    | source_name   |
    |---:|:--------------|
    |  0 | Diogo iPhone  |
    |  1 | Mi Fit        |
    |  2 | iPhone        |
    """

    def __init__(self, question_func: QuestionFunc, view_config: ViewConfig = None):
        if isinstance(question_func, Question):
            question_func = question_func.question_func

        self.question_func = question_func

        if view_config is None:
            self.view_config = ViewConfig(kind=ViewKind.table, title=self.name)
        else:
            self.view_config = view_config

    def __call__(self, *args, **kwargs):
        return self.question_func(*args, **kwargs)

    @property
    def name(self) -> str:
        """
        Human readable question name. E.g.:

        ```python
        @question
        def what_is_the_most_active_user():
            ...

        >>> what_is_the_most_active_user.name
        "What is the most active user?"


        @question
        def question_2():
            \"""
            What is the answer to the Ultimate Question of Life, the Universe, and Everything?
            \"""
            return select(literal(42))

        >>> question_2.name
        "What is the answer to the Ultimate Question of Life, the Universe, and Everything?"
        ```
        """
        if self.question_func.__doc__:
            return self.question_func.__doc__.strip()
        elif self.question_func.__name__ == "<lambda>":
            raise NotImplementedError
        else:
            question = self.question_func.__name__.replace("_", " ")
            return question.capitalize() + "?"

    @property
    def sql(self) -> str:
        """
        Returns the Question as SQL. E.g:

        ```sql
        SELECT sum(`step_count_by_source`.`value_count`) AS `total`
        FROM `amora-data-build-tool.amora`.`step_count_by_source`
        ```
        """
        stmt = self.question_func()
        return compile_statement(stmt)

    def answer_df(self) -> pd.DataFrame:
        """
        Executes the question against the target database,
        returning a `pandas.DataFrame` as the answer.

        ```python
        what_is_the_current_estimated_walked_distance.answer_df()
        ```

        ```
           total_in_centimeters  total_in_meters  total_in_kilometers   source_name
        0            93030637.0        930306.37            930.30637  Diogo iPhone
        1           112413129.0       1124131.29           1124.13129        Mi Fit
        2            11644600.0        116446.00            116.44600        iPhone
        ```
        """
        result = run(self.question_func())
        return result.rows.to_dataframe()

    def render(self) -> View:
        """
        Renders the visual representation of the question's answer
        """
        return View(data=self.answer_df(), config=self.view_config)

    def to_markdown(self) -> str:
        return f"""
            ## {self.name}
            
            ```sql
            {self.sql}
            ```
            
            ### Answer
            
            {self.answer_df().to_markdown()}
        """

    def _repr_markdown_(self):
        return self.to_markdown()

    def __repr__(self):
        return self._repr_markdown_()

    def __eq__(self, other):
        if not isinstance(other, Question):
            return False

        return self.question_func == other.question_func


QUESTIONS: List[Question] = []


def question(question_func: Callable):
    q = Question(question_func)
    QUESTIONS.append(q)

    return q