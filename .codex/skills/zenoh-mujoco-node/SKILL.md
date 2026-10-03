---
name: zenoh-mujoco-node
description: Use when building or refactoring a MuJoCo simulation node in this project's ROS-like ZenohNode framework, including passive viewer control, JSON publishers/subscribers, timers, and messages.py dataclasses. Do not use for generic MuJoCo rendering or standalone Zenoh scripts without ZenohNode.
---

# MuJoCo 节点与 ZenohNode

将仿真写成 `ZenohNode` 子类：节点持有模型和状态，在构造时声明发布者、订阅者和 Timer；`main()` 只负责创建、运行和关闭节点。先读取当前项目的 `zenoh_learn.node`、`zenoh_learn.executor`、`messages.py` 和模型，沿用现有接口与消息字段，不根据旧示例猜测协议。

## 节点结构

1. 在子类构造函数中加载 `MjModel` 和 `MjData`，建立 Zenoh 配置并调用 `super().__init__(name, config)`。使用 `create_json_publisher()`、`create_json_subscriber()`、`create_timer()` 声明资源；构造中途失败时，按项目现有节点模式关闭已经创建的资源。
2. 用 `messages.py` 的数据类转换消息：订阅回调用 `Torque.from_dict(data)`，发布前用 `Angle(...).to_dict()`、`Velocity(...).to_dict()`。key、端点和字段映射以当前项目及用户要求为准。
3. 在 Timer 回调中读取最新指令、写入 `data.ctrl`、调用 `mujoco.mj_step()`、发布状态并调用被动 viewer 的 `sync()`。viewer 关闭时调用 `stop()`，让 `spin()` 返回。`run()` 打开 `mujoco.viewer.launch_passive()` 并调用 `spin()`；`main()` 使用 `with Node() as node: node.run()`。
4. `ZenohNode` 的订阅回调和 Timer 回调由同一个 `spin()` 线程串行调度。不要为了共享扭矩数据增加接收线程或锁，也不要绕开框架改用原生 Zenoh Session，除非当前任务明确要求改变调度模型。

以下是结构示意；此处按当前 5 关节、6 字段模型展示映射，移植时先核对维度：

```python
class SimulationNode(ZenohNode):
    def __init__(self):
        self.model = mujoco.MjModel.from_xml_path(str(MODEL_PATH))
        self.data = mujoco.MjData(self.model)
        self.command = Torque(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        config = zenoh.Config()
        config.insert_json5("mode", '"peer"')
        config.insert_json5("scouting/multicast/enabled", "false")
        config.insert_json5("connect/endpoints", json.dumps([ZENOH_ENDPOINT]))
        super().__init__("simulation", config)
        try:
            self.angle_pub = self.create_json_publisher(ANGLE_KEY)
            self.velocity_pub = self.create_json_publisher(VELOCITY_KEY)
            self.command_sub = self.create_json_subscriber(COMMAND_KEY, self.on_command)
            self.timer = self.create_timer(self.model.opt.timestep, self.step)
        except BaseException as error:
            self.__exit__(type(error), error, error.__traceback__)
            raise

    def on_command(self, key, data):
        self.command = Torque.from_dict(data)

    def step(self):
        if not self.viewer.is_running():
            self.stop()
            return
        self.data.ctrl[:] = astuple(self.command)[:self.model.nu]
        mujoco.mj_step(self.model, self.data)
        angle = Angle(*(self.data.qpos.tolist() + [0.0]))
        velocity = Velocity(*(self.data.qvel.tolist() + [0.0]))
        self.angle_pub.publish(angle.to_dict())
        self.velocity_pub.publish(velocity.to_dict())
        self.viewer.sync()

    def run(self):
        with mujoco.viewer.launch_passive(self.model, self.data) as self.viewer:
            self.spin()


def main():
    with SimulationNode() as node:
        node.run()
```

## 时间与验证

- `model.opt.timestep` 是每次 `mj_step()` 推进的仿真时间。Timer 的周期只是目标频率；回调超时后 Executor 会跳过错过的周期。若每步都发布和 `viewer.sync()` 导致仿真变慢，保留物理步长，改为一个较低频率的 Timer 回调内执行多个物理步，再发布与同步画面。不要仅把 MuJoCo 时间步长调大来掩盖性能问题。
- 核对模型的 `nu`、`nq`、`nv` 与消息字段数。当前示例模型有 5 个关节和执行器，消息有 6 个电机字段：只用前 5 个扭矩，角度和速度的第 6 个字段填 `0.0`；其他模型须重新确认映射。
- 用真实 Zenoh peer 和无窗口 viewer 测试指令接收、角度/速度发布、订阅回调与 Timer 在 `spin()` 线程执行，以及关闭 viewer 后退出。图形窗口的实际帧率另行测量，不能由无窗口测试推断。
