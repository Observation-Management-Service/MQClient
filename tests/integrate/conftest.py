"""Fixtures."""


import os

import pytest_asyncio
from rest_tools.client import ClientCredentialsAuth


def do_skip_auth() -> bool:
    """Return whether to skip all the auth setup."""
    if os.getenv("PYTEST_DO_AUTH_FOR_MQCLIENT", None) == "no":
        return True
    elif os.getenv("PYTEST_DO_AUTH_FOR_MQCLIENT", None) != "yes":
        raise ValueError(
            f"PYTEST_DO_AUTH_FOR_MQCLIENT must be 'yes' or 'no' ({os.getenv('PYTEST_DO_AUTH_FOR_MQCLIENT')})"
        )
    # PYTEST_DO_AUTH_FOR_MQCLIENT is 'yes'
    return False


@pytest_asyncio.fixture
async def auth_token() -> str:
    """Get a valid token from Keycloak test instance."""
    if do_skip_auth():
        return ""

    cc = ClientCredentialsAuth(
        "",
        token_url=os.environ["KEYCLOAK_OIDC_URL"],
        client_id=os.environ["KEYCLOAK_CLIENT_ID"],
        client_secret=os.environ["KEYCLOAK_CLIENT_SECRET"],
    )
    token = cc.make_access_token()
    return token
