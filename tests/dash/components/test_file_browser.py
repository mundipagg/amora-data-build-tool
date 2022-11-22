from pathlib import Path

from dash.development.base_component import Component

from amora.dash.components.file_browser import layout


def test_component():
    component = layout()
    assert isinstance(component, Component)
    assert component.to_plotly_json()


def test_component_with_custom_path():
    component = layout(root=Path(__file__))
    assert isinstance(component, Component)
    assert component.to_plotly_json()
