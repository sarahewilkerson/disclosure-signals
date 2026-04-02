from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar


T = TypeVar("T")


def retry_call(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    backoff_seconds: float = 1.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    should_retry: Callable[[BaseException], bool] | None = None,
) -> T:
    last_error: BaseException | None = None
    for attempt in range(attempts):
        try:
            return fn()
        except retry_on as exc:
            last_error = exc
            if should_retry is not None and not should_retry(exc):
                raise
            if attempt == attempts - 1:
                raise
            time.sleep(backoff_seconds * (2 ** attempt))
    assert last_error is not None
    raise last_error
