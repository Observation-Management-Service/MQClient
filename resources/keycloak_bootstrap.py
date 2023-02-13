"""Script for creating a Keycloak realm."""


import time

from krs import bootstrap  # type: ignore[import]

TRIES = 5
for i in range(TRIES):
    try:
        client_secret = bootstrap.bootstrap()  # may fail if server isn't set up yet
    except:  # noqa: E722
        if i == TRIES - 1:
            raise
    time.sleep(i + 1)  # increasing/forgiving back-off

with open("KEYCLOAK_CLIENT_SECRET.txt", "w") as f:
    print(client_secret, file=f)
