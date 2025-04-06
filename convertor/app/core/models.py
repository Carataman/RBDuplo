from dataclasses import dataclass, asdict
from typing import Optional, List

@dataclass
class ViolationData:
    """Класс для хранения данных о нарушении"""
    v_azimut: float = 0.0
    v_camera: Optional[str] = None
    v_camera_serial: Optional[str] = None
    v_camera_place: Optional[str] = None
    v_direction: str = ""
    v_direction_name: str = ""
    v_gps_x: float = 0.0
    v_gps_y: float = 0.0
    v_regno: str = ""
    v_regno_country_id: Optional[str] = None
    v_speed: Optional[int] = None
    v_speed_limit: Optional[int] = None
    v_time_check: str = ""
    v_car_type: Optional[str] = None
    v_ts_model: Optional[str] = None
    v_patrol_speed: int = 56
    v_pr_viol: List[str] = None
    violation: Optional[str] = None
    v_photo_ts: str = ""
    v_photo_extra: List[str] = None

    def to_dict(self) -> dict:
        """Преобразует объект в словарь"""
        return asdict(self)

    def __post_init__(self):
        """Инициализация списков после создания объекта"""
        if self.v_photo_extra is None:
            self.v_photo_extra = []
        if self.v_pr_viol is None:
            self.v_pr_viol = []