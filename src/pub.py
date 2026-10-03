"""Publish a sample robot state every 0.1 seconds on demo/example."""

import time

import zenoh
from typing import Any

from messages import RobotState
from zenoh_learn.node import ZenohNode


class PublisherNode(ZenohNode):
    def __init__(self) -> None:
        # 为发布节点创建独立配置；peer 模式让业务节点直接通信，无需 zenohd。
        config = zenoh.Config()
        config.insert_json5('mode', '"peer"')
        # 不依赖 UDP 组播自动发现，改用下方明确指定的 TCP 地址。
        config.insert_json5('scouting/multicast/enabled', 'false')
        # 连接订阅节点的监听地址；127.0.0.1 表示两端运行在同一台电脑。
        config.insert_json5('connect/endpoints', '["tcp/127.0.0.1:17447"]')
        super().__init__('counter_publisher', config)
        try:
            self.publisher = self.create_json_publisher('demo/example')
            self.count = 0
            self.timer = self.create_timer(0.1, self.publish_state)
            self.subscription = self.create_json_subscriber('demo/sub', self.on_message)
        except BaseException as error:
            self.__exit__(type(error), error, error.__traceback__)
            raise

    def publish_state(self) -> None:
        state = RobotState(
            robot_id='robot_1',
            motor_1=float(self.count),
            motor_2=0.0,
            motor_3=0.0,
            motor_4=0.0,
            motor_5=0.0,
            motor_6=0.0,
            is_auto=True,
            timestamp=int(time.time() * 1000),
        )
        self.publisher.publish(state.to_dict())
        self.count += 1
    def on_message(self, key: str, data: dict[str, Any]) -> None:
        state = RobotState.from_dict(data)
        print(f'{state}', flush=True)

def main() -> None:
    try:
        with PublisherNode() as node:
            print('Publishing on demo/example every 0.1 s. Press Ctrl-C to stop.', flush=True)
            node.spin()
    except KeyboardInterrupt:
        print('Publisher stopped.', flush=True)


if __name__ == '__main__':
    main()
