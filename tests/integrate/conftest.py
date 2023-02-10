"""Fixtures."""

# type: skip-file
# fmt:quotes-ok

import json
import os
from functools import partial

import pytest
import pytest_asyncio
from krs import bootstrap
from krs.token import get_token
from rest_tools.client import ClientCredentialsAuth, RestClient


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


@pytest.fixture
def keycloak_bootstrap(monkeypatch):
    """Tools for Keycloack auth integration.

    From https://github.com/WIPACrepo/http-data-transfer-client/blob/main/integration_tests/util.py
    """
    if do_skip_auth():
        return None

    monkeypatch.setenv("KEYCLOAK_REALM", "testrealm")
    monkeypatch.setenv("KEYCLOAK_CLIENT_ID", "testclient")
    # monkeypatch.setenv("USERNAME", "admin")  # set in CI job
    # monkeypatch.setenv("PASSWORD", "admin")  # set in CI job

    secret = bootstrap.bootstrap()
    monkeypatch.setenv("KEYCLOAK_CLIENT_SECRET", secret)

    # get admin rest client
    token = partial(
        get_token,
        os.environ["KEYCLOAK_URL"],
        client_id="testclient",
        client_secret=secret,
    )
    rest_client = RestClient(
        f'{os.environ["KEYCLOAK_URL"]}/auth/admin/realms/testrealm',
        token=token,
        retries=0,
    )

    async def make_client(
        client_id,
        enable_secret=True,
        service_accounts_enabled=False,
        optional_client_scopes=None,
    ):
        # now make http client
        args = {
            "authenticationFlowBindingOverrides": {},
            "bearerOnly": False,
            "clientAuthenticatorType": "client-secret" if enable_secret else "public",
            "clientId": client_id,
            "consentRequired": False,
            "defaultClientScopes": [],
            "directAccessGrantsEnabled": False,
            "enabled": True,
            "frontchannelLogout": False,
            "fullScopeAllowed": True,
            "implicitFlowEnabled": False,
            "notBefore": 0,
            "optionalClientScopes": optional_client_scopes
            if optional_client_scopes
            else [],
            "protocol": "openid-connect",
            "publicClient": False,
            "redirectUris": ["http://localhost*"],
            "serviceAccountsEnabled": service_accounts_enabled,
            "standardFlowEnabled": True,
        }
        await rest_client.request("POST", "/clients", args)

        url = f"/clients?clientId={client_id}"
        ret = await rest_client.request("GET", url)
        if not ret:
            raise Exception("client does not exist")
        data = ret[0]
        keycloak_client_id = data["id"]

        # add mappers
        url = f'/clients/{keycloak_client_id}/protocol-mappers/add-models'
        args = [
            {
                'config': {
                    'access.token.claim': 'true',
                    'access.tokenResponse.claim': 'false',
                    'claim.name': 'authorization_details',
                    'claim.value': json.dumps(
                        [
                            {
                                "type": "rabbitmq",
                                "locations": ["cluster:*/vhost:*"],
                                "actions": ["read", "write", "configure"],
                            },
                            {
                                "type": "rabbitmq",
                                "locations": ["cluster:*"],
                                "actions": ["administrator"],
                            },
                        ]
                    ),
                    'id.token.claim': 'false',
                    'jsonType.label': 'JSON',
                    'userinfo.token.claim': 'false',
                },
                'consentRequired': False,
                'name': 'rich access',
                'protocol': 'openid-connect',
                'protocolMapper': 'oidc-hardcoded-claim-mapper',
            },
            {
                'config': {
                    'access.token.claim': 'true',
                    'id.token.claim': 'false',
                    'included.custom.audience': 'rabbitmq_client',
                },
                'consentRequired': False,
                'name': 'aud-rabbitmq_client',
                'protocol': 'openid-connect',
                'protocolMapper': 'oidc-audience-mapper',
            },
            {
                'config': {
                    'access.token.claim': 'true',
                    'id.token.claim': 'false',
                    'included.custom.audience': 'rabbitmq',
                },
                'consentRequired': False,
                'name': 'aud-rabbitmq',
                'protocol': 'openid-connect',
                'protocolMapper': 'oidc-audience-mapper',
            },
        ]
        await rest_client.request("POST", url, args)

        # set up return values
        args = {
            "oidc_url": f'{os.environ["KEYCLOAK_URL"]}/auth/realms/testrealm',
            "client_id": client_id,
        }
        if enable_secret:
            url = f'/clients/{keycloak_client_id}/client-secret'
            ret = await rest_client.request("GET", url)
            if "value" in ret:
                args["client_secret"] = ret["value"]
            else:
                raise Exception("no client secret")

        return args

    yield make_client

    tok = bootstrap.get_token()
    bootstrap.delete_service_role("testclient", token=tok)
    bootstrap.delete_realm("testrealm", token=tok)


@pytest_asyncio.fixture
async def auth_token(keycloak_bootstrap) -> str:
    """Get a valid token from Keycloak test instance."""
    if do_skip_auth():
        return ""

    kwargs = await keycloak_bootstrap(
        "mqclient-integration-test",
        enable_secret=True,
        service_accounts_enabled=True,
        optional_client_scopes=["profile", "offline_access"],
    )

    cc = ClientCredentialsAuth(
        "",
        token_url=kwargs["oidc_url"],
        client_id=kwargs["client_id"],
        client_secret=kwargs["client_secret"],
    )
    token = cc.make_access_token()
    return token
