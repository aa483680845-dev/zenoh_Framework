"""Reusable JSON publishers and subscribers for an active Zenoh session."""

import json
import logging
import math
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import zenoh

if TYPE_CHECKING:
    from .executor import Executor

logger = logging.getLogger(__name__)


def _validate_capacity(capacity: int) -> None:
    if isinstance(capacity, bool) or not isinstance(capacity, int):
        raise TypeError('buffer_capacity must be an integer')
    if capacity <= 0:
        raise ValueError('buffer_capacity must be positive')


def _finite_float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('JSON numbers must be finite')
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f'Invalid JSON constant: {value}')


class JsonPublisher:
    """Publish JSON dictionaries on one key of a caller-owned session."""

    def __init__(self, session: zenoh.Session, key: str,
                 executor: 'Executor | None' = None) -> None:
        self._executor = executor
        if executor is not None:
            executor._check_can_create()
        self._closed = False
        self._publisher = session.declare_publisher(
            key, encoding=zenoh.Encoding.APPLICATION_JSON
        )

    def publish(self, data: dict[str, Any]) -> None:
        if self._executor is not None:
            self._executor._check_owner()
        if self._closed:
            raise RuntimeError('publisher is closed')
        if not isinstance(data, dict):
            raise TypeError('published data must be a dictionary')
        self._publisher.put(json.dumps(data, ensure_ascii=False, allow_nan=False))

    def close(self) -> None:
        if self._executor is not None:
            self._executor._check_owner()
        if self._closed:
            return
        self._closed = True
        self._publisher.undeclare()

    def __enter__(self) -> 'JsonPublisher':
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()


class JsonSubscriber:
    """Deliver objects as callback(key, data), optionally on an Executor.

    Only executor mode allocates a native ring buffer. Direct callbacks run
    on Zenoh threads; buffer_capacity is validated but unused in that mode.
    """

    def __init__(
        self,
        session: zenoh.Session,
        key_expr: str,
        callback: Callable[[str, dict[str, Any]], None],
        executor: 'Executor | None' = None,
        *,
        buffer_capacity: int = 100,
    ) -> None:
        _validate_capacity(buffer_capacity)
        self._closed = False
        self._callback = callback
        self._executor = executor
        if executor is not None:
            executor._check_can_create()
        handler = zenoh.handlers.RingChannel(buffer_capacity) if executor else self._on_sample
        self._subscriber = session.declare_subscriber(key_expr, handler)
        if executor is not None:
            executor._add_subscriber(self)

    def _on_sample(self, sample: zenoh.Sample) -> None:
        if self._closed:
            return
        try:
            data = json.loads(sample.payload.to_string(), parse_constant=_reject_constant,
                              parse_float=_finite_float)
        except (UnicodeDecodeError, ValueError, RecursionError):
            logger.warning('Ignoring invalid JSON on %s', sample.key_expr)
            return
        if not isinstance(data, dict):
            logger.warning('Ignoring non-object JSON on %s', sample.key_expr)
            return
        self._callback(str(sample.key_expr), data)

    def _dispatch_one(self) -> bool:
        if self._closed:
            return False
        sample = self._subscriber.try_recv()
        if sample is None:
            return False
        self._on_sample(sample)
        return True

    def close(self) -> None:
        if self._closed:
            return
        if self._executor is not None:
            self._executor._check_owner()
        self._closed = True
        if self._executor is not None:
            self._executor._remove_subscriber(self)
        self._subscriber.undeclare()

    def __enter__(self) -> 'JsonSubscriber':
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()
