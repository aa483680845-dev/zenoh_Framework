"""接收 Zenoh 扭矩指令，显示仿真并发布关节角度和速度。"""

from dataclasses import astuple
import json
from pathlib import Path
from typing import Any

import mujoco
import zenoh

from messages import Angle, Torque, Velocity
from zenoh_learn.node import ZenohNode


MODEL_PATH = Path(__file__).resolve().parent.parent / "model" / "mjcf_v2.0" / "robot.xml"
COMMAND_KEY = "robot/arm/command"
ANGLE_KEY = "robot/arm/angle"
VELOCITY_KEY = "robot/arm/velocity"
ZENOH_ENDPOINT = "tcp/127.0.0.1:17447"


class MujocoNode(ZenohNode):
    def __init__(self) -> None:
        self.model = mujoco.MjModel.from_xml_path(str(MODEL_PATH))
        self.data = mujoco.MjData(self.model)
        self.torques = [0.0] * 6

        # 作为 peer 主动连接监听端；收发共用这一条连接，不依赖组播发现。
        config = zenoh.Config()
        config.insert_json5("mode", '"peer"')
        config.insert_json5("scouting/multicast/enabled", "false")
        config.insert_json5("connect/endpoints", json.dumps([ZENOH_ENDPOINT]))
        super().__init__("mujoco_robot", config)
        try:
            self.angle_publisher = self.create_json_publisher(ANGLE_KEY)
            self.velocity_publisher = self.create_json_publisher(VELOCITY_KEY)
            self.subscription = self.create_json_subscriber(COMMAND_KEY, self.on_command)
            self.timer = self.create_timer(self.model.opt.timestep, self.step)
        except BaseException as error:
            self.__exit__(type(error), error, error.__traceback__)
            raise

    def on_command(self, _key: str, payload: dict[str, Any]) -> None:
        command = Torque.from_dict(payload)
        self.torques[:] = astuple(command)

    def step(self) -> None:
        if not self.viewer.is_running():
            self.stop()
            return
        # robot.xml 中有五个执行器，因此只使用前五个扭矩值。
        self.data.ctrl[:] = self.torques[:self.model.nu]
        mujoco.mj_step(self.model, self.data)
        # 消息定义有六个电机，模型只有五个关节，第六个值填 0。
        angle = Angle(*(self.data.qpos.tolist() + [0.0]))
        velocity = Velocity(*(self.data.qvel.tolist() + [0.0]))
        self.angle_publisher.publish(angle.to_dict())
        self.velocity_publisher.publish(velocity.to_dict())
        self.viewer.sync()

    def run(self) -> None:
        import mujoco.viewer

        with mujoco.viewer.launch_passive(self.model, self.data) as self.viewer:
            self.spin()


def main() -> None:
    try:
        with MujocoNode() as node:
            node.run()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
