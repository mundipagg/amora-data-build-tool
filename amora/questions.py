import unicodedata
from datetime import date
from typing import Callable, List, Optional, Set

import pandas as pd
from sqlalchemy import Column

from amora.compilation import compile_statement
from amora.protocols import Compilable
from amora.providers import bigquery
from amora.storage import cache
from amora.visualization import Table, Visualization, VisualizationConfig

QuestionFunc = Callable[[], Compilable]


def parse_name(question_name: str, replace_non_alpha_to: str = "_"):
    """
    Parses a string to a valid Python function name.

    Args:
        question_name: Question name to parse

    Returns:
        A valid Python function name
    """
    s = question_name.replace("?", "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.encode("ascii", "ignore").decode("ascii")
    s = "".join(c if c.isalnum() else replace_non_alpha_to for c in s)
    s = s.lower()
    return s


class Question:
    """
    A Data Question is a __query__, its __results__, and its __visualization__.

    The __query__ is expressed as an `amora.protocols.Compilable`, its __results__ is a
    `pandas.DataFrame` and the results __visualization__ as an `amora.visualization.Visualization`.

    ## Example

    Lets define a new data question:

    ```python
    from examples.models.step_count_by_source import StepCountBySource

    from amora.models import select
    from amora.questions import question


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

    def __init__(
        self,
        question_func: QuestionFunc,
        view_config: Optional[VisualizationConfig] = None,
    ):
        if isinstance(question_func, Question):
            question_func = question_func.question_func
        self.question_func = question_func
        self.view_config = view_config or Table(title=self.name)

    def __call__(self, *args, **kwargs):
        return self.question_func(*args, **kwargs)

    @property
    def name(self) -> str:
        '''
        Human readable question name. E.g.:

        ```python
        @question
        def what_is_the_most_active_user():
            ...
        ```

        Would result in the parsed function name:
        ```
        >>> what_is_the_most_active_user.name
        "What is the most active user?"
        ```

        But if a question has a docstring, it will be used as the question name:

        ```python
        @question
        def question_2():
            """
            What is the answer to the Ultimate Question of Life, the Universe, and Everything?
            """
            return select(literal(42))
        ```

        ```
        >>> question_2.name
        "What is the answer to the Ultimate Question of Life, the Universe, and Everything?"
        ```
        '''
        if self.question_func.__doc__:
            return self.question_func.__doc__.strip()

        if self.question_func.__name__ == "<lambda>":
            raise NotImplementedError

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

    @cache(suffix=lambda self: f"{self.question_func.__name__}.{date.today()}")
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
        result = bigquery.run(self.question_func())
        return result.rows.to_dataframe(create_bqstorage_client=False)

    def answer_columns(self) -> List[Column]:
        """
        Returns an iterable of SQLAlchemy `Column` objects that correspond to the columns returned by executing the
        SQL query associated with this `Question` instance.

        Returns:
            Iterable[sqlalchemy.sql.schema.Column]: An iterable of `Column` objects that correspond to the columns returned
            by executing the SQL query associated with this `Question` instance.

        Raises:
            google.api_core.exceptions.GoogleAPIError: If the query job fails.
        """
        result = bigquery.dry_run_query(self.sql)
        return [bigquery.column_for_schema_field(field) for field in result.schema]

    @property
    def uid(self) -> str:
        return str(hash(self))

    def render(self) -> Visualization:
        """
        Renders the visual representation of the question's answer
        """
        return Visualization(data=self.answer_df(), config=self.view_config)

    def to_markdown(self) -> str:
        return "\n".join(
            [
                f"## {self.name}",
                f"```sql \n {self.sql} \n```",
                f"### Answer",
                self.answer_df().to_markdown(),
            ]
        )

    def to_html(self):
        return f"""
            <h2>{self.name}</h2>

            <pre><code>{self.sql}</code></pre>

            <h3>Answer</h3>

            {self.answer_df().to_html()}
        """

    def _repr_markdown_(self):
        return self.to_markdown()

    def __repr__(self):
        return self._repr_markdown_()

    def __eq__(self, other):
        if not isinstance(other, Question):
            return False
        return hash(self) == hash(other)

    def __hash__(self):
        code = self.question_func.__code__
        return hash(code.co_filename + code.co_name)


QUESTIONS: Set[Question] = set()


def question(view_config: Optional[VisualizationConfig] = None):
    """
    Wraps the function into a `amora.questions.Question`.
    The decorated function must return a `Compilable`

    E.g:
    ```python
    @question()
    def what_are_the_available_data_sources():
        return select(StepCountBySource.source_name).distinct()
    ```

    Optional data visualization configuration can be passed using the `view_config` arg:

    ```python
    from amora.visualization import PieChart


    @question(view_config=PieChart(values="total", names="source_name"))
    def what_is_the_total_step_count_to_date():
        return select(
            func.sum(StepCountBySource.value_sum).label("total"),
            StepCountBySource.source_name,
        ).group_by(StepCountBySource.source_name)
    ```

    """

    def decorator(question_func: QuestionFunc) -> Question:
        q = Question(question_func, view_config=view_config)
        QUESTIONS.add(q)

        return q

    return decorator
