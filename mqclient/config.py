"""Package-wide config."""


DEFAULT_TIMEOUT = 60
DEFAULT_EXCEPT_ERRORS = True
DEFAULT_PREFETCH = 0
DEFAULT_RETRIES = 0  # TODO: fix
DEFAULT_RETRY_DELAY = 1

DEFAULT_TIMEOUT_MILLIS = 1000  # milliseconds


# NOTE - it's tempting to add environment variables, but since
# an application is likely to have multiple Queue instances,
# env vars are best handled on the app-side (not the API)
