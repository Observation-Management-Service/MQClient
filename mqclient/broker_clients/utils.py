"""Utilities."""


import asyncio
import inspect
import logging
from typing import Awaitable, Callable, Optional, TypeVar, Union

T = TypeVar("T")  # the callable/awaitable return type


def _ci_test_retry_trigger(i: int) -> None:
    pass


async def auto_retry_call(
    factory: Callable[[], Union[T, Awaitable[T]]],
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
        func = factory()
        try:
            _ci_test_retry_trigger(i)  # only used for testing
            ret = func()
            if inspect.isawaitable(ret):
                return await ret  # type: ignore[no-any-return]
            else:
                return ret  # type: ignore[return-value]
        except Exception as e:
            logger.exception(e)
            if nonretriable_conditions and nonretriable_conditions(e):
                raise
            elif i == retries:
                logger.info(
                    f"[auto_retry_call()] {type(e)}. Reached max retries. Raising..."
                )
                raise
            else:
                logger.info(
                    f"[auto_retry_call()] {type(e)}. Trying again. (attempt #{i+2})..."
                )

        # close, wait, reconnect
        try:
            await close()  # the previous error could've been due to a closed connection
        except:  # noqa: E722
            pass
        await asyncio.sleep(retry_delay)
        await connect()

    # fall through -- this should not be reached in any situation
    raise Exception("Max retries exceeded / connection error")
