from urllib.parse import quote_plus, urlencode

from authlib.integrations.flask_client import OAuth
from dash import Dash
from flask import redirect, session, url_for

from amora.dash.config import settings


def add_auth0_login(dash_app: Dash) -> None:
    flask_app = dash_app.server
    flask_app.secret_key = settings.APP_SECRET_KEY
    oauth = OAuth(flask_app)

    oauth.register(
        "auth0",
        client_id=settings.AUTH0_CLIENT_ID,
        client_secret=settings.AUTH0_CLIENT_SECRET,
        client_kwargs={
            "scope": settings.AUTH0_SCOPE,
        },
        server_metadata_url=f"https://{settings.AUTH0_DOMAIN}/.well-known/openid-configuration",
    )

    @flask_app.route(settings.AUTH0_LOGIN_PATH)
    def login():
        return oauth.auth0.authorize_redirect(
            redirect_uri=url_for("callback", _external=True)
        )

    @flask_app.route(settings.AUTH0_CALLBACK_PATH, methods=["GET", "POST"])
    def callback():
        token = oauth.auth0.authorize_access_token()
        session["user"] = token
        return redirect(settings.AUTH0_CALLBACK_REDIRECT_PATH)

    @flask_app.route(settings.AUTH0_LOGOUT_PATH)
    def logout():
        session.clear()
        return redirect(
            "https://"
            + settings.AUTH0_DOMAIN
            + "/v2/logout?"
            + urlencode(
                {
                    "returnTo": url_for(
                        settings.AUTH0_CALLBACK_REDIRECT_PATH, _external=True
                    ),
                    "client_id": settings.AUTH0_CLIENT_ID,
                },
                quote_via=quote_plus,
            )
        )
