import json
import socket
import threading
import unittest
from unittest.mock import patch

import mujoco.viewer
import zenoh

from mujoco_playground import mujoco_view
from messages import Torque
from zenoh_learn.pubsub import JsonPublisher, JsonSubscriber


class FakeViewer:
    def __init__(self):
        self.steps = 0
        self.data = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def is_running(self):
        self.steps += 1
        return self.steps <= 200

    def sync(self):
        if self.steps <= 20 and self.steps % 5 == 0:
            self.command.publish(Torque(0.5, 0, 0, 0, 0, 0).to_dict())


class MujocoViewTests(unittest.TestCase):
    def test_direct_peer_receives_torque_and_publishes_angle_and_velocity(self):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            endpoint = f'tcp/127.0.0.1:{sock.getsockname()[1]}'

        config = zenoh.Config()
        config.insert_json5('mode', '"peer"')
        config.insert_json5('scouting/multicast/enabled', 'false')
        config.insert_json5('listen/endpoints', json.dumps([endpoint]))
        angles, velocities = [], []
        viewer = FakeViewer()
        owner_thread = threading.get_ident()
        command_threads = []
        original_on_command = mujoco_view.MujocoNode.on_command

        def record_command(node, key, payload):
            command_threads.append(threading.get_ident())
            original_on_command(node, key, payload)

        def launch_viewer(model, data):
            viewer.data = data
            return viewer

        with zenoh.open(config) as session:
            with (JsonSubscriber(session, 'robot/arm/angle', lambda key, data: angles.append(data)),
                  JsonSubscriber(session, 'robot/arm/velocity', lambda key, data: velocities.append(data)),
                  JsonPublisher(session, 'robot/arm/command') as command):
                viewer.command = command
                with (patch.object(mujoco_view, 'ZENOH_ENDPOINT', endpoint),
                      patch.object(mujoco_view.MujocoNode, 'on_command', record_command),
                      patch.object(mujoco.viewer, 'launch_passive', side_effect=launch_viewer)):
                    mujoco_view.main()

        self.assertTrue(command_threads)
        self.assertEqual(set(command_threads), {owner_thread})
        self.assertTrue(angles)
        self.assertTrue(velocities)
        self.assertEqual(set(angles[-1]), {f'motor_{i}' for i in range(1, 7)})
        self.assertEqual(set(velocities[-1]), {f'motor_{i}' for i in range(1, 7)})
        self.assertEqual(angles[-1]['motor_6'], 0.0)
        self.assertEqual(velocities[-1]['motor_6'], 0.0)
        self.assertAlmostEqual(viewer.data.ctrl[0], 0.5)
