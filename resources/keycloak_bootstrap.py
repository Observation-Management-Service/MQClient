"""Script for creating a Keycloak realm."""


from krs import bootstrap  # type: ignore[import]

bootstrap.bootstrap()
