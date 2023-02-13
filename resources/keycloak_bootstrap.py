"""Script for creating a Keycloak realm."""

import asyncio
import json
import os
import time
from functools import partial

from krs import bootstrap  # type: ignore[import]
from krs.token import get_token  # type: ignore[import]
from rest_tools.client import RestClient

TRIES = 5


def _bootstrap() -> str:
    # may fail if server isn't set up yet
    for i in range(TRIES):
        try:
            return bootstrap.bootstrap()  # type: ignore[no-any-return]
        except:  # noqa: E722
            if i == TRIES - 1:
                raise
        time.sleep(i + 1)  # increasing/forgiving back-off
    raise RuntimeError("Too many failures")


async def keycloak_bootstrap(
    client_id,
    enable_secret=True,
    service_accounts_enabled=False,
    optional_client_scopes=None,
) -> dict:
    """Tools for Keycloack auth integration.

    From https://github.com/WIPACrepo/http-data-transfer-client/blob/main/integration_tests/util.py
    """
    client_secret = _bootstrap()

    # monkeypatch.setenv("KEYCLOAK_REALM", "testrealm")  # set env by CI job
    # monkeypatch.setenv("KEYCLOAK_CLIENT_ID", "testclient")  # set env by CI job
    # monkeypatch.setenv("USERNAME", "admin")  # set env by CI job
    # monkeypatch.setenv("PASSWORD", "admin")  # set env by CI job

    # get admin rest client
    token = partial(
        get_token,
        os.environ["KEYCLOAK_URL"],
        client_id=os.environ["KEYCLOAK_CLIENT_ID"],
        client_secret=client_secret,
    )
    rest_client = RestClient(
        f'{os.environ["KEYCLOAK_URL"]}/auth/admin/realms/{os.environ["KEYCLOAK_REALM"]}',
        token=token,
        retries=0,
    )

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
    url = f"/clients/{keycloak_client_id}/protocol-mappers/add-models"
    args = [
        {
            "config": {
                "access.token.claim": "true",
                "access.tokenResponse.claim": "false",
                "claim.name": "authorization_details",
                "claim.value": json.dumps(
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
                "id.token.claim": "false",
                "jsonType.label": "JSON",
                "userinfo.token.claim": "false",
            },
            "consentRequired": False,
            "name": "rich access",
            "protocol": "openid-connect",
            "protocolMapper": "oidc-hardcoded-claim-mapper",
        },
        {
            "config": {
                "access.token.claim": "true",
                "id.token.claim": "false",
                "included.custom.audience": "rabbitmq_client",
            },
            "consentRequired": False,
            "name": "aud-rabbitmq_client",
            "protocol": "openid-connect",
            "protocolMapper": "oidc-audience-mapper",
        },
        {
            "config": {
                "access.token.claim": "true",
                "id.token.claim": "false",
                "included.custom.audience": "rabbitmq",
            },
            "consentRequired": False,
            "name": "aud-rabbitmq",
            "protocol": "openid-connect",
            "protocolMapper": "oidc-audience-mapper",
        },
    ]
    await rest_client.request("POST", url, args)

    # set up return values
    args = {
        "oidc_url": f'{os.environ["KEYCLOAK_URL"]}/auth/realms/{os.environ["KEYCLOAK_REALM"]}',
        "client_id": client_id,
    }
    if enable_secret:
        url = f"/clients/{keycloak_client_id}/client-secret"
        ret = await rest_client.request("GET", url)
        if "value" in ret:
            args["client_secret"] = ret["value"]
        else:
            raise Exception("no client secret")

    return args


async def main() -> None:
    """Do main."""
    kwargs = await keycloak_bootstrap(
        "mqclient-integration-test",
        enable_secret=True,
        service_accounts_enabled=True,
        optional_client_scopes=["profile", "offline_access"],
    )

    # write to files
    with open("KEYCLOAK_OIDC_URL.txt", "w") as f:
        print(kwargs["oidc_url"], file=f)

    with open("KEYCLOAK_CLIENT_ID.txt", "w") as f:
        print(kwargs["client_id"], file=f)

    with open("KEYCLOAK_CLIENT_SECRET.txt", "w") as f:
        print(kwargs["client_secret"], file=f)


if __name__ == "__main__":
    asyncio.run(main())
