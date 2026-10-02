import json
import threading
import unittest
from uuid import uuid4

import zenoh

from zenoh_learn.pubsub import JsonPublisher, JsonSubscriber


class JsonPubSubTests(unittest.TestCase):
    def test_round_trip_delivers_key_and_json_object(self):
        key = f"demo/test/{uuid4().hex}"
        received = []
        ready = threading.Event()

        with zenoh.open(zenoh.Config()) as session:
            with JsonSubscriber(session, key, lambda k, data: (received.append((k, data)), ready.set())):
                with JsonPublisher(session, key) as publisher:
                    publisher.publish({"message": "你好", "count": 2})
                    self.assertTrue(ready.wait(3), "subscriber did not receive the message")

        self.assertEqual(received, [(key, {"message": "你好", "count": 2})])

    def test_invalid_json_does_not_reach_callback(self):
        key = f"demo/test/{uuid4().hex}"
        received = []
        valid_received = threading.Event()

        with zenoh.open(zenoh.Config()) as session:
            with JsonSubscriber(session, key, lambda k, data: (received.append(data), valid_received.set())):
                session.put(key, "not-json")
                session.put(key, json.dumps({"valid": True}))
                self.assertTrue(valid_received.wait(3), "valid message did not arrive")

        self.assertEqual(received, [{"valid": True}])

    def test_closed_subscriber_receives_no_new_messages(self):
        key = f"demo/test/{uuid4().hex}"
        received = threading.Event()

        with zenoh.open(zenoh.Config()) as session:
            with JsonSubscriber(session, key, lambda k, data: received.set()):
                pass
            with JsonPublisher(session, key) as publisher:
                publisher.publish({"after": "close"})
                self.assertFalse(received.wait(0.2))


if __name__ == "__main__":
    unittest.main()
