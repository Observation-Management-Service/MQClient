"""Utilities."""


import asyncio
import inspect
import logging
from typing import Awaitable, Callable, Optional, TypeVar, Union

from .. import log_msgs

T = TypeVar("T")  # the callable/awaitable return type


async def auto_retry_call(
    func: Callable[[], Union[T, Awaitable[T]]],
    retries: int,
    retry_delay: int,
    close: Callable[[], Awaitable[None]],
    connect: Callable[[], Awaitable[None]],
    logger: logging.Logger,
    nonretriable_conditions: Optional[Callable[[Exception], bool]] = None,
) -> T:
    """Call `func` with auto-retries."""
    retry_delay = max(retry_delay, 1)
    retries = max(retries, 0)

    for i in range(retries + 1):
        if i > 0:
            logger.debug(
                f"{log_msgs.TRYCALL_CONNECTION_ERROR_TRY_AGAIN} (attempt #{i+1})..."
            )

        try:
            ret = func()
            if inspect.isawaitable(ret):
                return await ret  # type: ignore[no-any-return]
            else:
                return ret  # type: ignore[return-value]
        except Exception as e:
            logger.error(e)
            if nonretriable_conditions and nonretriable_conditions(e):
                raise
            elif i == retries:
                logger.debug(log_msgs.TRYCALL_CONNECTION_ERROR_MAX_RETRIES)
                raise
            else:
                pass

        # close, wait, reconnect
        try:
            await close()  # the previous error could've been due to a closed connection
        except:  # noqa: E722
            pass
        await asyncio.sleep(retry_delay)
        await connect()

    # fall through -- this should not be reached in any situation
    logger.debug(log_msgs.TRYCALL_CONNECTION_ERROR_MAX_RETRIES)
    raise Exception("Max retries exceeded / connection error")
