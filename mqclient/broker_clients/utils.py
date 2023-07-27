"""Utilities."""


import asyncio
import inspect
import logging
from typing import Awaitable, Callable, Optional, TypeVar, Union

from ..broker_client_interface import MQClientException

T = TypeVar("T")  # the callable/awaitable return type


def _ci_test_retry_trigger(i: int) -> None:
    pass


async def auto_retry_call(
    func: Callable[[], Union[T, Awaitable[T]]],
    retries: int,
    retry_delay: float,
    logger: logging.Logger,
    close: Optional[Callable[[], Awaitable[None]]],
    connect: Optional[Callable[[], Awaitable[None]]],
    nonretriable_conditions: Optional[Callable[[Exception], bool]],
) -> T:
    """Call `func` with auto-retries."""
    retry_delay = max(retry_delay, 0.01)
    retries = max(retries, 0)

    for i in range(retries + 1):
        try:
            _ci_test_retry_trigger(i)  # only used for testing
            ret = func()
            if inspect.isawaitable(ret):
                return await ret  # type: ignore[no-any-return]
            else:
                return ret  # type: ignore[return-value]
        except Exception as e:
            logger.error(f"Broker client action failed with {type(e).__name__}")
            logger.exception(e)
            if nonretriable_conditions and nonretriable_conditions(e):
                logger.error(
                    f"Broker client action failure ({type(e).__name__}) is not retriable"
                )
                raise
            elif i == retries:
                logger.error(
                    f"Broker client action failure ({type(e).__name__}) was final try -- raising..."
                )
                raise
            else:
                logger.warning(
                    f"Broker client action failure ({type(e).__name__}) is retriable -- trying again (attempt #{i+2})..."
                )

        # close, wait, reconnect
        if close:
            try:
                await close()  # the previous error could've been due to a closed connection
            except:  # noqa: E722
                pass
        await asyncio.sleep(retry_delay)
        if connect:
            await connect()

    # fall through -- this should not be reached in any situation
    raise MQClientException("unknown error in auto_retry_call()")
