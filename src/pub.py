"""Publish an incrementing count every 0.1 seconds on demo/example."""

from zenoh_learn.node import ZenohNode


class PublisherNode(ZenohNode):
    def __init__(self) -> None:
        super().__init__('counter_publisher')
        try:
            self.publisher = self.create_json_publisher('demo/example')
            self.count = 0
            self.timer = self.create_timer(0.1, self.publish_count)
        except BaseException as error:
            self.__exit__(type(error), error, error.__traceback__)
            raise

    def publish_count(self) -> None:
        self.publisher.publish({'count': self.count})
        self.count += 1


def main() -> None:
    try:
        with PublisherNode() as node:
            print('Publishing on demo/example every 0.1 s. Press Ctrl-C to stop.', flush=True)
            node.spin()
    except KeyboardInterrupt:
        print('Publisher stopped.', flush=True)


if __name__ == '__main__':
    main()
