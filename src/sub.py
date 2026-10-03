"""Print the source key and JSON data from a subscriber node."""

from typing import Any

import zenoh

from messages import RobotState
from zenoh_learn.node import ZenohNode


class SubscriberNode(ZenohNode):
    def __init__(self) -> None:
        # 为订阅节点创建独立配置；peer 模式让业务节点直接通信，无需 zenohd。
        config = zenoh.Config()
        config.insert_json5('mode', '"peer"')
        # 不依赖 UDP 组播自动发现，等待发布节点主动连接。
        config.insert_json5('scouting/multicast/enabled', 'false')
        # 在本机 TCP 端口监听；发布节点的 connect/endpoints 必须指向这里。
        config.insert_json5('listen/endpoints', '["tcp/127.0.0.1:17447"]')
        super().__init__('counter_subscriber', config)
        try:
            self.subscription = self.create_json_subscriber('demo/example', self.on_message)
        except BaseException as error:
            self.__exit__(type(error), error, error.__traceback__)
            raise

    def on_message(self, key: str, data: dict[str, Any]) -> None:
        state = RobotState.from_dict(data)
        print(f'{state}', flush=True)


def main() -> None:
    try:
        with SubscriberNode() as node:
            print('Listening on demo/example. Press Ctrl-C to stop.', flush=True)
            node.spin()
    except KeyboardInterrupt:
        print('Subscriber stopped.', flush=True)


if __name__ == '__main__':
    main()
