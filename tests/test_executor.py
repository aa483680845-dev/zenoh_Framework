import math
import threading
import time
import unittest
from unittest.mock import patch

from zenoh_learn.executor import Executor


class ExecutorTests(unittest.TestCase):
    def test_timer_deadline_and_skips_missed_periods(self):
        calls = []
        with patch('zenoh_learn.executor.time.monotonic', return_value=10) as clock:
            executor = Executor()
            executor.create_timer(1, lambda: calls.append(clock.return_value))
            clock.return_value = 10.9
            executor._run_once()
            self.assertEqual(calls, [])
            clock.return_value = 14.5
            executor._run_once()
            executor._run_once()
            self.assertEqual(calls, [14.5])
            clock.return_value = 15
            executor._run_once()
            self.assertEqual(calls, [14.5, 15])

    def test_slow_callback_does_not_accumulate_timer_work(self):
        with patch('zenoh_learn.executor.time.monotonic', return_value=0) as clock:
            executor = Executor()
            calls = []
            def callback():
                calls.append(True)
                clock.return_value = 8.5
            executor.create_timer(1, callback)
            clock.return_value = 1
            executor._run_once()
            executor._run_once()
            self.assertEqual(calls, [True])
            clock.return_value = 9
            executor._run_once()
            self.assertEqual(calls, [True, True])

    def test_tiny_positive_periods_do_not_overflow_after_delay(self):
        for period in [5e-324, 1e-308, 0.1]:
            with self.subTest(period=period), patch('zenoh_learn.executor.time.monotonic', return_value=0) as clock:
                executor = Executor()
                calls = []
                executor.create_timer(period, lambda: calls.append(True))
                clock.return_value = 2
                executor._run_once()
                executor._run_once()
                self.assertEqual(calls, [True])

    def test_rounding_does_not_dispatch_before_next_planned_period(self):
        with patch('zenoh_learn.executor.time.monotonic', return_value=0) as clock:
            executor = Executor()
            calls = []
            executor.create_timer(0.1, lambda: calls.append(True))
            clock.return_value = 2
            executor._run_once()
            clock.return_value = 2.05
            executor._run_once()
            self.assertEqual(calls, [True])
            clock.return_value = 2.1
            executor._run_once()
            self.assertEqual(calls, [True, True])

    def test_period_below_clock_precision_is_scheduled_in_future(self):
        with patch('zenoh_learn.executor.time.monotonic', return_value=10) as clock:
            executor = Executor()
            calls = []
            executor.create_timer(5e-324, lambda: calls.append(True))
            executor._run_once()
            self.assertEqual(calls, [])
            clock.return_value = math.nextafter(10, math.inf)
            executor._run_once()
            self.assertEqual(calls, [True])

    def test_cancel_prevents_pending_timer_callback(self):
        with patch('zenoh_learn.executor.time.monotonic', return_value=0) as clock:
            executor = Executor()
            calls = []
            executor.create_timer(1, lambda: timer.cancel())
            timer = executor.create_timer(1, lambda: calls.append(True))
            clock.return_value = 1
            executor._run_once()
            timer.cancel()
            clock.return_value = 5
            executor._run_once()
            self.assertEqual(calls, [])

    def test_invalid_periods(self):
        executor = Executor()
        for value in [0, -1, float('inf'), -float('inf'), float('nan')]:
            with self.subTest(period=value), self.assertRaises(ValueError):
                executor.create_timer(value, lambda: None)
        for value in [True, '1', None]:
            with self.subTest(period=value), self.assertRaises(TypeError):
                executor.create_timer(value, lambda: None)

    def test_callback_exception_propagates_and_spin_resets_running_state(self):
        executor = Executor()
        error = ValueError('business callback')
        def callback():
            raise error
        executor.create_timer(0.001, callback)
        with self.assertRaises(ValueError) as caught:
            executor.spin()
        self.assertIs(caught.exception, error)
        self.assertFalse(executor._running)

    def test_stop_wakes_idle_wait(self):
        executor = Executor()
        # A longer wait makes this check distinguish Event.wait from time.sleep.
        executor._idle_wait = 1
        stopper = threading.Thread(target=lambda: (time.sleep(0.02), executor.stop()))
        stopper.start()
        started = time.monotonic()
        executor.spin()
        elapsed = time.monotonic() - started
        stopper.join(1)
        self.assertLess(elapsed, 0.3)

    def test_nearest_timer_shortens_idle_wait(self):
        executor = Executor()
        executor._idle_wait = 1
        executor.create_timer(0.01, executor.stop)
        started = time.monotonic()
        executor.spin()
        self.assertLess(time.monotonic() - started, 0.3)

    def test_callbacks_run_serially_on_spin_thread(self):
        executor = Executor()
        owner = threading.get_ident()
        calls = []
        executor.create_timer(0.001, lambda: calls.append(threading.get_ident()))
        executor.create_timer(0.002, executor.stop)
        executor.spin()
        self.assertTrue(calls)
        self.assertEqual(set(calls), {owner})

    def test_no_resource_creation_during_spin(self):
        executor = Executor()
        executor.create_timer(0.001, lambda: executor.create_timer(1, lambda: None))
        with self.assertRaises(RuntimeError):
            executor.spin()
