import threading
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

import zenoh

from zenoh_learn.node import ZenohNode
from zenoh_learn.executor import Executor
from zenoh_learn.pubsub import JsonSubscriber


def local_config():
    config = zenoh.Config()
    config.insert_json5('scouting/multicast/enabled', 'false')
    return config


class NodeTests(unittest.TestCase):
    def test_native_buffers_keep_latest_and_are_independent_and_fair(self):
        for capacity, count, expected in [(None, 105, list(range(5, 105))),
                                          (3, 6, [3, 4, 5]), (1, 3, [2])]:
            with self.subTest(capacity=capacity), ZenohNode('buffers', local_config()) as node:
                key = f'test/{uuid4().hex}'
                calls = []
                kwargs = {} if capacity is None else {'buffer_capacity': capacity}
                node.create_json_subscriber(key + '/a', lambda k, d: calls.append(('a', d['i'])), **kwargs)
                node.create_json_subscriber(key + '/b', lambda k, d: calls.append(('b', d['i'])), buffer_capacity=1)
                pub_a = node.create_json_publisher(key + '/a')
                pub_b = node.create_json_publisher(key + '/b')
                for i in range(count):
                    pub_a.publish({'i': i})
                for i in range(3):
                    pub_b.publish({'i': i})
                node.create_timer(0.05, node.stop)
                node.spin()
                self.assertEqual([i for k, i in calls if k == 'a'], expected)
                self.assertEqual([i for k, i in calls if k == 'b'], [2])
                self.assertEqual(calls[:2], [('a', expected[0]), ('b', 2)])

    def test_timer_precedes_subscriber_and_closed_pending_sub_is_skipped(self):
        with ZenohNode('ordering', local_config()) as node:
            key = f'test/{uuid4().hex}'
            calls = []
            sub = node.create_json_subscriber(key, lambda k, d: calls.append('message'))
            node.create_json_publisher(key).publish({'ready': True})
            with patch('zenoh_learn.executor.time.monotonic', return_value=0) as clock:
                node.create_timer(1, lambda: (calls.append('timer'), sub.close()))
                clock.return_value = 1
                node._executor._run_once()
            self.assertEqual(calls, ['timer'])

    def test_messages_are_validated_on_spin_thread_and_callback_errors_escape(self):
        with ZenohNode('callbacks', local_config()) as node:
            key = f'test/{uuid4().hex}'
            calls = []
            error = LookupError('callback failed')
            def callback(k, d):
                calls.append((k, d, threading.get_ident()))
                raise error
            node.create_json_subscriber(key, callback)
            for payload in ['bad', '[]', '{"x":NaN}', '{"x":1e999}', '{"ok":true}']:
                node._session.put(key, payload)
            with self.assertLogs('zenoh_learn.pubsub', level='WARNING'):
                with self.assertRaises(LookupError) as caught:
                    node.spin()
            self.assertIs(caught.exception, error)
            self.assertEqual(calls, [(key, {'ok': True}, threading.get_ident())])

    def test_node_rejects_capacity_before_declaration(self):
        with patch('zenoh_learn.node.zenoh.open') as open_session:
            with ZenohNode('validation') as node:
                for value, error in [(True, TypeError), (1.2, TypeError), (0, ValueError), (-1, ValueError)]:
                    with self.assertRaises(error):
                        node.create_json_subscriber('test/json', lambda k, d: None, buffer_capacity=value)
                open_session.return_value.declare_subscriber.assert_not_called()

    def test_cleanup_continues_and_reports_errors_and_close_is_idempotent(self):
        events = []
        session = Mock()
        session.declare_subscriber.return_value.undeclare.side_effect = lambda: (events.append('sub'), (_ for _ in ()).throw(ValueError('sub cleanup')))
        session.declare_publisher.return_value.undeclare.side_effect = lambda: (events.append('pub'), (_ for _ in ()).throw(RuntimeError('pub cleanup')))
        session.close.side_effect = lambda: events.append('session')
        with patch('zenoh_learn.node.zenoh.open', return_value=session):
            node = ZenohNode('cleanup')
            pub = node.create_json_publisher('test/json')
            node.create_json_subscriber('test/json', lambda k, d: None)
            timer = node.create_timer(1, lambda: None)
            with self.assertRaises(ExceptionGroup) as caught:
                node.close()
            self.assertEqual(len(caught.exception.exceptions), 2)
            self.assertEqual(events, ['sub', 'pub', 'session'])
            node.close()
            self.assertTrue(timer._cancelled)
            for operation in [lambda: node.create_json_publisher('test/json'),
                              lambda: node.create_json_subscriber('test/json', lambda k, d: None),
                              lambda: node.create_timer(1, lambda: None), node.spin,
                              lambda: pub.publish({'x': 1})]:
                with self.assertRaises(RuntimeError):
                    operation()

    def test_body_and_cleanup_errors_both_preserve_original_exceptions(self):
        body_error = LookupError('body')
        cleanup_error = ValueError('session close')
        session = Mock()
        session.close.side_effect = cleanup_error
        with patch('zenoh_learn.node.zenoh.open', return_value=session):
            with self.assertRaises(ExceptionGroup) as caught:
                with ZenohNode('combined_errors'):
                    raise body_error
        self.assertIs(caught.exception.exceptions[0], body_error)
        self.assertIs(caught.exception.exceptions[1].exceptions[0], cleanup_error)
        session.close.assert_called_once()

    def test_close_during_spin_is_rejected_without_losing_cleanup(self):
        with patch('zenoh_learn.node.zenoh.open', return_value=Mock()) as opened:
            with ZenohNode('close') as node:
                node.create_timer(.001, node.close)
                with self.assertRaises(RuntimeError):
                    node.spin()
            opened.return_value.close.assert_called_once()

    def test_only_stop_allowed_from_other_threads(self):
        with patch('zenoh_learn.node.zenoh.open', return_value=Mock()):
            with ZenohNode('thread') as node:
                errors = []
                def other_thread():
                    for operation in [node.close, node.spin, lambda: node.create_timer(1, lambda: None)]:
                        try:
                            operation()
                        except RuntimeError:
                            errors.append(True)
                    node.stop()
                thread = threading.Thread(target=other_thread)
                thread.start()
                thread.join(1)
                self.assertEqual(errors, [True, True, True])
                node.spin()

    def test_node_owned_publisher_rejects_cross_thread_operations(self):
        with patch('zenoh_learn.node.zenoh.open', return_value=Mock()) as opened:
            with ZenohNode('publisher_thread') as node:
                publisher = node.create_json_publisher('test/json')
                errors = []
                def other_thread():
                    for operation in [lambda: publisher.publish({'x': 1}), publisher.close]:
                        try:
                            operation()
                        except RuntimeError:
                            errors.append(True)
                thread = threading.Thread(target=other_thread)
                thread.start()
                thread.join(1)
                self.assertEqual(errors, [True, True])
                opened.return_value.declare_publisher.return_value.put.assert_not_called()
                opened.return_value.declare_publisher.return_value.undeclare.assert_not_called()
                publisher.publish({'x': 2})

    def test_standalone_executor_close_allows_subscriber_cleanup(self):
        executor = Executor()
        sub = JsonSubscriber(Mock(), 'test/json', lambda k, d: None, executor)
        executor.close()
        sub.close()
