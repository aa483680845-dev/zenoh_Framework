"""Single-thread scheduling of native subscriber buffers and periodic timers."""

import math
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .pubsub import JsonSubscriber


class Timer:
    """A periodic monotonic timer; cancel on the executor's owner thread."""

    def __init__(self, period: float, callback: Callable[[], None], executor: 'Executor') -> None:
        if isinstance(period, bool) or not isinstance(period, (int, float)):
            raise TypeError('timer period must be a number')
        if not math.isfinite(period) or period <= 0:
            raise ValueError('timer period must be finite and positive')
        self._period = period
        self._callback = callback
        self._executor = executor
        now = time.monotonic()
        self._deadline = now + period
        if self._deadline <= now:
            self._deadline = math.nextafter(now, math.inf)
        self._cancelled = False

    def cancel(self) -> None:
        self._executor._check_owner()
        self._cancelled = True

    def _dispatch_due(self, now: float) -> bool:
        if self._cancelled or now < self._deadline:
            return False
        self._callback()
        # Advance the planned deadline beyond callback completion. Late periods
        # are skipped, including time spent inside a slow callback.
        now = time.monotonic()
        elapsed_periods = (now - self._deadline) / self._period
        if math.isfinite(elapsed_periods):
            skipped = max(1, math.floor(elapsed_periods) + 1)
            self._deadline += skipped * self._period
            # Division can round the count down by one at an exact deadline.
            if self._deadline <= now:
                self._deadline += self._period
        else:
            # Extremely small periods can overflow the quotient. A remainder
            # avoids representing the (potentially enormous) skipped count.
            self._deadline = now + (self._period - ((now - self._deadline) % self._period))
        if self._deadline <= now:
            self._deadline = math.nextafter(now, math.inf)
        return True


class Executor:
    """Run callbacks serially. Only stop() may be called from another thread.

    Create resources before spin(). A stop request is persistent, so stop()
    immediately before spin() also prevents dispatch.
    """

    def __init__(self) -> None:
        self._owner = threading.get_ident()
        self._subscribers: list[JsonSubscriber] = []
        self._timers: list[Timer] = []
        self._stopped = threading.Event()
        self._running = False
        self._closed = False
        self._idle_wait = 0.01

    def _check_owner(self) -> None:
        if threading.get_ident() != self._owner:
            raise RuntimeError('only stop() may be called from another thread')

    def _check_can_create(self) -> None:
        self._check_owner()
        if self._closed:
            raise RuntimeError('executor is closed')
        if self._running:
            raise RuntimeError('create resources before spin()')

    def _add_subscriber(self, subscriber: 'JsonSubscriber') -> None:
        self._check_can_create()
        self._subscribers.append(subscriber)

    def _remove_subscriber(self, subscriber: 'JsonSubscriber') -> None:
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)

    def create_timer(self, period: float, callback: Callable[[], None]) -> Timer:
        self._check_can_create()
        timer = Timer(period, callback, self)
        self._timers.append(timer)
        return timer

    def _run_once(self) -> bool:
        did_work = False
        for timer in tuple(self._timers):
            if self._stopped.is_set():
                return did_work
            did_work = timer._dispatch_due(time.monotonic()) or did_work
        for subscriber in tuple(self._subscribers):
            if self._stopped.is_set():
                break
            did_work = subscriber._dispatch_one() or did_work
        return did_work

    def spin(self) -> None:
        self._check_can_create()
        self._running = True
        try:
            while not self._stopped.is_set():
                if self._run_once():
                    continue
                now = time.monotonic()
                delay = min((max(0.0, timer._deadline - now)
                             for timer in self._timers if not timer._cancelled),
                            default=self._idle_wait)
                self._stopped.wait(min(self._idle_wait, delay))
        finally:
            self._running = False

    def stop(self) -> None:
        self._stopped.set()

    def close(self) -> None:
        self._check_owner()
        if self._running:
            raise RuntimeError('close() must be called after spin() returns')
        if self._closed:
            return
        self.stop()
        self._closed = True
        for timer in self._timers:
            timer.cancel()
        self._timers.clear()
        # Subscribers are owned by their caller (normally ZenohNode).
        self._subscribers.clear()
