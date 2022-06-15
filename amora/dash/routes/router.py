from typing import Callable, Dict

from dash import html
from dash.development.base_component import Component

from amora.dash.routes import environement, feature_store, home, models, questions

ROUTER: Dict[str, Callable[..., Component]] = {
    "/": home.content,
    "/search": lambda: html.P("This is the content of page 1. Yay!"),
    "/models": models.content,
    "/questions": questions.content,
    "/whiteboard": lambda: html.P("Whiteboard"),
    "/environment": environement.content,
    "/feature-store": feature_store.content,
}
