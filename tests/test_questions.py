import pandas
import pytest
from sqlalchemy import literal

from amora.models import select
from amora.questions import question, QUESTIONS, Question
from amora.types import Compilable


def test_Question_is_compared_through_question_function():
    def a_question() -> Compilable:
        """
        What is the answer to the ultimate question of life, the universe, and everything?
        """
        return select(literal(42).label("answer"))

    def another_question() -> Compilable:
        """
        What is the answer to the ultimate question of life, the universe, and everything?
        """
        return select(literal(42).label("answer"))

    assert Question(a_question) != a_question
    assert Question(a_question) == question(a_question)
    assert Question(a_question) == Question(a_question)
    assert Question(a_question) != Question(another_question)


def test_Question_question_func():
    def a_question() -> Compilable:
        return select(literal(42).label("answer"))

    assert Question(a_question).question_func == a_question
    assert Question(Question(a_question)).question_func == a_question
    assert Question(Question(Question(a_question))).question_func == a_question


def test_Question_name():
    def a_question() -> Compilable:
        """
        What is Diogo's favorite fruit?
        """
        return select(literal("amora").label("answer"))

    assert Question(a_question).name == "What is Diogo's favorite fruit?"

    def another_question() -> Compilable:
        return select(literal(42).label("answer"))

    assert Question(another_question).name == "Another question?"

    lambda_question = lambda: select(literal(42).label("answer"))

    with pytest.raises(NotImplementedError):
        Question(lambda_question).name


def test_Question_sql():
    def a_question() -> Compilable:
        """
        What is Diogo's favorite fruit?
        """
        return select(literal("amora").label("answer"))

    assert Question(a_question).sql == "SELECT 'amora' AS `answer`"


def test_Question_answer_df():
    @question
    def a_question() -> Compilable:
        return select(literal("amora").label("answer"))

    assert (a_question.answer_df() == pandas.DataFrame([{"answer": "amora"}])).bool()


def test_question_decorator_storages_the_Question():
    def a_question() -> Compilable:
        """
        What is the answer to the ultimate question of life, the universe, and everything?
        """
        return select(literal(42).label("answer"))

    assert Question(a_question) not in QUESTIONS

    question(a_question)

    assert Question(a_question) in QUESTIONS


def test_question_decorator_wraps_the_question_function_on_a_Question():
    def a_question() -> Compilable:
        """
        What is the answer to the ultimate question of life, the universe, and everything?
        """
        return select(literal(42).label("answer"))

    wrapped_question = question(a_question)
    assert isinstance(wrapped_question, Question)