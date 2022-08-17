import dash_mantine_components as dmc
from authlib.oidc.core import UserInfo
from dash import dcc
from dash.development.base_component import Component

from amora.dash.config import Size


def layout(user_info: UserInfo, size: Size = Size.large) -> Component:
    return dcc.Link(
        dmc.Avatar(
            src=user_info["picture"],
            alt=user_info["given_name"],
            radius=Size.extra_large,
            size=size,
        ),
        href=f"/users/{user_info['email']}",
        title=user_info["name"],
    )
