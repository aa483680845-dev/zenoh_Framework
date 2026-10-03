from dataclasses import asdict, dataclass
from typing import Any, Self


@dataclass
class RobotState:
    robot_id: str
    motor_1: float
    motor_2: float
    motor_3: float
    motor_4: float
    motor_5: float
    motor_6: float
    is_auto: bool
    timestamp: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls(**data)
