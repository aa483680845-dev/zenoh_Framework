"""Print the source key and JSON data from a subscriber node."""

from typing import Any

from zenoh_learn.node import ZenohNode


class SubscriberNode(ZenohNode):
    def __init__(self) -> None:
        super().__init__('counter_subscriber')
        try:
            self.subscription = self.create_json_subscriber('demo/example', self.on_message)
        except BaseException as error:
            self.__exit__(type(error), error, error.__traceback__)
            raise

    def on_message(self, key: str, data: dict[str, Any]) -> None:
        print(f'{key}: {data}', flush=True)


def main() -> None:
    try:
        with SubscriberNode() as node:
            print('Listening on demo/example. Press Ctrl-C to stop.', flush=True)
            node.spin()
    except KeyboardInterrupt:
        print('Subscriber stopped.', flush=True)


if __name__ == '__main__':
    main()
