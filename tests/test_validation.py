import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from zenoh_learn.pubsub import JsonPublisher, JsonSubscriber
from zenoh_learn.executor import Executor


class ValidationTests(unittest.TestCase):
    def test_filters_invalid_object_and_nonfinite_json(self):
        session = Mock()
        received = []
        sub = JsonSubscriber(session, 'test/json', lambda k, d: received.append((k, d)))
        handler = session.declare_subscriber.call_args.args[1]
        for payload in ['not-json', '[]', 'null', '{"x":NaN}', '{"x":Infinity}',
                        '{"x":-Infinity}', '{"x":[1e999]}']:
            sample = SimpleNamespace(key_expr='test/json', payload=SimpleNamespace(to_string=lambda: payload))
            with self.assertLogs('zenoh_learn.pubsub', level='WARNING'):
                handler(sample)
        handler(SimpleNamespace(key_expr='test/json', payload=SimpleNamespace(to_string=lambda: '{"ok":1}')))
        self.assertEqual(received, [('test/json', {'ok': 1})])
        sub.close()

    def test_capacity_rejected_before_declaration_in_both_modes(self):
        for capacity, error in [(True, TypeError), (False, TypeError), (1.5, TypeError),
                                ('1', TypeError), (0, ValueError), (-1, ValueError)]:
            for executor in [None, Executor()]:
                with self.subTest(capacity=capacity, executor=executor):
                    session = Mock()
                    with self.assertRaises(error):
                        JsonSubscriber(session, 'test/json', lambda k, d: None, executor, buffer_capacity=capacity)
                    session.declare_subscriber.assert_not_called()

    def test_close_is_idempotent_and_publishing_after_close_rejected(self):
        session = Mock()
        pub = JsonPublisher(session, 'test/json')
        sub = JsonSubscriber(session, 'test/json', lambda k, d: None)
        pub.close()
        pub.close()
        sub.close()
        sub.close()
        session.declare_publisher.return_value.undeclare.assert_called_once()
        session.declare_subscriber.return_value.undeclare.assert_called_once()
        with self.assertRaises(RuntimeError):
            pub.publish({'x': 1})

    def test_publish_rejects_nonobjects_and_nonfinite_values(self):
        pub = JsonPublisher(Mock(), 'test/json')
        with self.assertRaises(TypeError):
            pub.publish([])
        for value in [float('nan'), float('inf'), -float('inf')]:
            with self.assertRaises(ValueError):
                pub.publish({'nested': [value]})
        pub.close()
