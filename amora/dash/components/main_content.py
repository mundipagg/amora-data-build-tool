from dash import html

CONTENT_STYLE = {
    "margin-left": "24rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
    "overflow": "scroll",
}

content = html.Div(id="page-content", style=CONTENT_STYLE)
