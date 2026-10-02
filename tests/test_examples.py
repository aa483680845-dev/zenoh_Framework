import importlib.util
from pathlib import Path
import queue
import signal
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]


def load_example(name):
    spec = importlib.util.spec_from_file_location(f'example_{name}', ROOT / 'src' / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ExampleTests(unittest.TestCase):
    def test_publisher_constructor_failure_closes_publisher_and_session(self):
        module = load_example('pub')
        session = Mock()
        with patch('zenoh_learn.node.zenoh.open', return_value=session), \
                patch.object(module.PublisherNode, 'create_timer', side_effect=ValueError('timer setup')):
            with self.assertRaises(ValueError):
                module.PublisherNode()
        session.declare_publisher.return_value.undeclare.assert_called_once()
        session.close.assert_called_once()

    def test_subscriber_constructor_failure_closes_session(self):
        module = load_example('sub')
        session = Mock()
        session.declare_subscriber.side_effect = ValueError('subscriber setup')
        with patch('zenoh_learn.node.zenoh.open', return_value=session):
            with self.assertRaises(ValueError):
                module.SubscriberNode()
        session.close.assert_called_once()

    def test_two_processes_publish_counts_and_exit_on_ctrl_c(self):
        lines = queue.Queue()
        processes = []
        readers = []
        def start(name):
            process = subprocess.Popen([sys.executable, '-u', str(ROOT / 'src' / f'{name}.py')],
                                       cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(process)
            def read():
                for line in process.stdout:
                    lines.put((name, line.strip()))
            reader = threading.Thread(target=read, daemon=True)
            reader.start()
            readers.append(reader)
            return process
        def wait_line(name, predicate):
            deadline = time.monotonic() + 12
            while time.monotonic() < deadline:
                try:
                    source, line = lines.get(timeout=max(.001, deadline - time.monotonic()))
                except queue.Empty:
                    break
                if source == name and predicate(line):
                    return line
            self.fail(f'{name} did not produce expected output within 12 seconds')
        try:
            subscriber = start('sub')
            wait_line('sub', lambda line: 'Listening' in line)
            publisher = start('pub')
            messages = [wait_line('sub', lambda line: line.startswith('demo/example:')) for _ in range(3)]
            # Parse the printed Python dictionaries without executing text.
            import ast
            counts = [ast.literal_eval(line.split(': ', 1)[1])['count'] for line in messages]
            self.assertEqual(counts, list(range(counts[0], counts[0] + 3)))
            for process in [publisher, subscriber]:
                process.send_signal(signal.SIGINT)
                self.assertEqual(process.wait(timeout=5), 0)
                self.assertEqual(process.stderr.read(), '')
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
            for reader in readers:
                reader.join(timeout=2)
            for process in processes:
                process.stdout.close()
                process.stderr.close()
