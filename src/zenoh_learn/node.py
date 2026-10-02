"""A learning-oriented node owning one Zenoh session and one executor."""

from collections.abc import Callable
from typing import Any

import zenoh

from .executor import Executor, Timer
from .pubsub import JsonPublisher, JsonSubscriber


class ZenohNode:
    """Create resources, spin, then close on one thread; stop from any thread."""

    def __init__(self, name: str, config: zenoh.Config | None = None) -> None:
        self.name = name
        self._executor = Executor()
        self._publishers: list[JsonPublisher] = []
        self._subscribers: list[JsonSubscriber] = []
        self._closed = False
        self._session = zenoh.open(config if config is not None else zenoh.Config())

    def _check_can_create(self) -> None:
        if self._closed:
            raise RuntimeError('node is closed')
        self._executor._check_can_create()

    def create_json_publisher(self, key: str) -> JsonPublisher:
        self._check_can_create()
        publisher = JsonPublisher(self._session, key, executor=self._executor)
        self._publishers.append(publisher)
        return publisher

    def create_json_subscriber(
        self,
        key_expr: str,
        callback: Callable[[str, dict[str, Any]], None],
        *,
        buffer_capacity: int = 100,
    ) -> JsonSubscriber:
        self._check_can_create()
        subscriber = JsonSubscriber(self._session, key_expr, callback, self._executor,
                                    buffer_capacity=buffer_capacity)
        self._subscribers.append(subscriber)
        return subscriber

    def create_timer(self, period: float, callback: Callable[[], None]) -> Timer:
        self._check_can_create()
        return self._executor.create_timer(period, callback)

    def spin(self) -> None:
        self._check_can_create()
        self._executor.spin()

    def stop(self) -> None:
        self._executor.stop()

    def close(self) -> None:
        self._executor._check_owner()
        if self._executor._running:
            raise RuntimeError('close() must be called after spin() returns')
        if self._closed:
            return
        self._closed = True
        self.stop()
        errors = []
        # Each close is attempted even if an earlier resource failed. Marking
        # wrappers closed before undeclare makes a later close harmless.
        for resource in [*self._subscribers, *self._publishers, self._executor, self._session]:
            try:
                resource.close()
            except BaseException as error:
                errors.append(error)
        self._subscribers.clear()
        self._publishers.clear()
        if errors:
            raise BaseExceptionGroup(f'Errors closing node {self.name!r}', errors)

    def __enter__(self) -> 'ZenohNode':
        self._check_can_create()
        return self

    def __exit__(self, exc_type: object, exc_value: BaseException | None, traceback: object) -> None:
        try:
            self.close()
        except BaseException as cleanup_error:
            if exc_value is not None:
                raise BaseExceptionGroup('Node body and cleanup failed', [exc_value, cleanup_error]) from None
            raise
