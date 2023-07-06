"""Utilities."""


import asyncio
import logging
from typing import Awaitable, Callable, TypeVar

from .. import log_msgs
from ..broker_client_interface import RETRY_DELAY, TRY_ATTEMPTS

T = TypeVar("T")  # the callable/awaitable return type


async def try_call(
    func: Callable[[], Awaitable[T]],
    nonretriable_conditions: Callable[[Exception], bool],
    # retriable_conditions: Callable[[Exception], bool],
    close: Callable[[], Awaitable[None]],
    connect: Callable[[], Awaitable[None]],
    logger: logging.Logger,
) -> T:
    """Call `func` with auto-retries."""
    for i in range(TRY_ATTEMPTS):
        if i > 0:
            logger.debug(
                f"{log_msgs.TRYCALL_CONNECTION_ERROR_TRY_AGAIN} (attempt #{i+1})..."
            )

        try:
            return await func()
        except Exception as e:
            logger.error(e)
            if nonretriable_conditions(e):
                raise
            elif i + 1 == TRY_ATTEMPTS:
                logger.debug(log_msgs.TRYCALL_CONNECTION_ERROR_MAX_RETRIES)
                raise
            else:
                pass

        # close, wait, reconnect
        try:
            await close()  # the previous error could've been due to a closed connection
        except:  # noqa: E722
            pass
        await asyncio.sleep(RETRY_DELAY)
        await connect()

    # fall through -- this should not be reached in any situation
    logger.debug(log_msgs.TRYCALL_CONNECTION_ERROR_MAX_RETRIES)
    raise Exception("Max retries exceeded / connection error")
