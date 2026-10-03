# Zenoh 学习版节点框架

Python 3.12 上的 JSON 发布/订阅封装，增加 `ZenohNode`、单线程 `Executor`
和周期 `Timer`。每个 Node 拥有一个独立 Session，业务回调在 `spin()` 所在线程串行执行。

安装依赖：

```sh
uv sync
```

先启动一个监听 `tcp/127.0.0.1:17447` 的 Zenoh peer 节点，再运行 MuJoCo 仿真：

```sh
uv run python mujoco_playground/mujoco_view.py
```

仿真节点以 peer 模式连接该地址，在同一个 Zenoh Session 中订阅扭矩指令并发布关节状态：

| Key | 方向 | 消息类型 |
| --- | --- | --- |
| `robot/arm/command` | 接收 | `Torque` |
| `robot/arm/angle` | 发送 | `Angle` |
| `robot/arm/velocity` | 发送 | `Velocity` |

三种消息都使用 `src/messages.py` 中的 `motor_1` 至 `motor_6` 字段。模型有五个关节和五个执行器：
扭矩指令的第六个值不参与控制，角度和速度消息的第六个值填 `0.0`。跨电脑运行时，
将 `mujoco_view.py` 中的 `ZENOH_ENDPOINT` 改为监听节点可访问的地址。

`MujocoNode` 继承 `ZenohNode`，在构造时创建发布者、订阅者和按模型步长运行的 Timer。
Timer 回调推进 MuJoCo 仿真并同步被动 viewer；订阅回调和 Timer 回调都由同一线程的
`spin()` 调度。关闭 viewer 后节点调用 `stop()` 退出。

## Node 接口

```python
from zenoh_learn.node import ZenohNode

with ZenohNode('state_monitor') as node:
    state_sub = node.create_json_subscriber(
        'robot/state',
        lambda key, data: print(key, data),
        buffer_capacity=1,
    )
    publisher = node.create_json_publisher('robot/status')
    timer = node.create_timer(1.0, lambda: publisher.publish({'alive': True}))
    # timer.cancel() 可以取消定时器。
    try:
        node.spin()
    except KeyboardInterrupt:
        node.stop()
```

- `create_json_publisher(key)` 返回 `JsonPublisher`，使用 `publish(dict)` 发送。
- `create_json_subscriber(key_expr, callback, *, buffer_capacity=100)` 返回
  `JsonSubscriber`，回调签名为 `callback(key: str, data: dict)`。
- `create_timer(period, callback)` 返回可 `cancel()` 的 Timer；周期单位为秒，
  必须是有限正数，回调不接收参数。
- `spin()` 阻塞调度；`stop()` 请求退出并唤醒空闲等待，不会中断正在执行的回调。
- `close()` 停止调度、关闭订阅者和发布者，最后关闭 Session；重复关闭无副作用。

每个订阅拥有独立的 Zenoh 原生 `zenoh.handlers.RingChannel`，默认容量为 100，
满时淘汰最旧消息。`buffer_capacity=1` 适合只关心最新状态的订阅。
容量必须为正整数；非整数（包括布尔值）抛出 `TypeError`，零或负数抛出
`ValueError`，校验发生在声明订阅前。不同订阅可以分别配置容量。
不提供逐条淘汰日志或精确丢弃计数。

## 调度与生命周期

每轮先按创建顺序执行到期 Timer，再按创建顺序从每个订阅最多取一条消息，
解码、校验后执行回调。有工作时立即进入下一轮；空闲等待最长 10 ms，
最近的 Timer 截止时间可以缩短等待。`stop()` 使用事件唤醒等待。

Timer 使用单调时钟，按计划截止时间推进，跳过已经错过的周期（包括回调耗时），
每轮最多执行一次，不积压回调。
当周期小于浮点时钟的表示精度时，下一次截止时间使用时钟可表示的下一个时刻。取消 Timer 或关闭订阅后，其待调度任务不再执行。
JSON 必须是对象；无效 JSON、数组、标量，以及 `NaN`、`Infinity` 和溢出为无穷大的
数字（例如 `1e999`）会记录日志并跳过，嵌套对象中的数值也遵循此规则。
发布时也拒绝非对象和非有限数值。

业务回调异常立即退出 `spin()`，保留原始异常和堆栈，由调用者处理。
Node 在某项清理失败后继续清理其他资源，最终以 Python 3.12 的
`ExceptionGroup`（含系统异常时为 `BaseExceptionGroup`）报告错误。
如果业务执行和清理同时失败，`with` 保留两者。
关闭后创建资源、调用 `spin()` 或向已关闭的发布者发送消息都会抛出 `RuntimeError`。

在同一线程中构造 Node、创建资源、调用 `spin()`，并在 `spin()` 返回后执行
`close()`；资源必须在 `spin()` 前创建。其他线程只允许调用 `stop()`。
`stop()` 请求保持有效，因此提前调用 `stop()` 会使之后的 `spin()` 立即返回。
第一版不提供硬实时保证、多线程执行器或共享 Session；慢回调会延迟其他业务回调。

## 直接使用现有封装

原有导入路径和直接回调方式仍然可用：

```python
import threading
import zenoh
from zenoh_learn.pubsub import JsonPublisher, JsonSubscriber

received = threading.Event()

def on_message(key, data):
    print(key, data)
    received.set()

with zenoh.open(zenoh.Config()) as session:
    with JsonSubscriber(session, 'demo/example', on_message):
        with JsonPublisher(session, 'demo/example') as publisher:
            publisher.publish({'message': 'Hello from Zenoh'})
            received.wait(3)
```

直接模式的回调在 Zenoh 接收线程中执行，不创建本框架的消息缓冲区；
`buffer_capacity` 仍会校验，但仅在 Executor 接收模式下决定缓冲容量。
调用者负责保持 Session 存活，并先关闭订阅者和发布者。

也可以直接使用 Executor：

```python
from zenoh_learn.executor import Executor

with zenoh.open(zenoh.Config()) as session:
    executor = Executor()
    try:
        with JsonSubscriber(session, 'demo/example', on_message,
                            executor=executor, buffer_capacity=10):
            executor.create_timer(5, executor.stop)
            executor.spin()
    finally:
        executor.close()
```

独立 Executor 的 `close()` 取消 Timer 并停止调度，订阅者和 Session 由调用者关闭。

## 验证

```sh
uv run python -m unittest discover -s tests -v
```

测试包括收发、校验、原生环形缓冲区、调度、Timer、生命周期，以及使用真实 Zenoh
会话和无窗口 viewer 验证 MuJoCo 的扭矩接收、角度和速度发布。真实 Zenoh 测试需要
系统允许其共享内存和网络初始化。
