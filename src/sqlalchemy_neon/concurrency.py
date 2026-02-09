from __future__ import annotations

from typing import Awaitable, TypeVar
import greenlet

_T = TypeVar("_T")


def await_only_allow_missing(awaitable: Awaitable[_T]) -> _T:
    """
    SQLAlchemy의 greenlet_spawn 내부에서 동작하는 유틸리티.
    부모(greenlet_spawn의 루프)에게 awaitable을 전달하고,
    부모가 await를 완료한 뒤 결과를 돌려줄 때까지 대기합니다.
    """
    current = greenlet.getcurrent()
    if not current.parent:
        raise RuntimeError("Should be run inside greenlet_spawn")

    return current.parent.switch(awaitable)
