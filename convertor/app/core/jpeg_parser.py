import base64
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any, Union

from models import ViolationData


logger = logging.getLogger(__name__)


class JpegParser:
    """Парсер JPEG файлов с встроенными метаданными о нарушениях"""

    JPEG_MARKERS = (b'\xff\xd8', b'\xff\xd9')  # Маркеры начала/конца JPEG
    ENCODINGS = ['utf-8', 'windows-1251', 'latin-1']  # Поддерживаемые кодировки

    def __init__(self, config: Optional[Dict] = None):
        """Инициализация парсера с конфигурацией"""
        self.config = config or {}
        self._setup_defaults()

    def _setup_defaults(self):
        """Установка значений по умолчанию"""
        self.default_coord = 0.0
        self.default_speed = 0
        self.default_date = datetime(1970, 1, 1)

    def parse(self, jpeg_data: bytes) -> Optional[ViolationData]:
        """Основной метод парсинга"""
        try:
            self._validate_input(jpeg_data)

            jpeg_frames = self._extract_frames(jpeg_data)
            json_data = self._extract_json(jpeg_data)
            parsed_data = self._parse_json(json_data)

            return self._build_violation(jpeg_frames, parsed_data)

        except Exception as e:
            logger.error(f"Parser error: {e}", exc_info=True)
            return None

    def _validate_input(self, data: bytes):
        """Проверка входных данных"""
        if not data or len(data) < 10:
            raise ParserError("Invalid or empty input data")
        if not data.startswith(self.JPEG_MARKERS[0]):
            raise ParserError("Not a valid JPEG file")

    def _extract_frames(self, data: bytes) -> List[str]:
        """Извлечение JPEG фреймов"""
        frames = []
        pos = 0

        while pos < len(data):
            start_pos = data.find(self.JPEG_MARKERS[0], pos)
            if start_pos == -1: break

            end_pos = data.find(self.JPEG_MARKERS[1], start_pos)
            if end_pos == -1: break

            frame = data[start_pos:end_pos + 2]
            frames.append(base64.b64encode(frame).decode('utf-8'))
            pos = end_pos + 2

        return frames

    def _extract_json(self, data: bytes) -> bytes:
        """Извлечение JSON данных"""
        last_frame_end = data.rfind(self.JPEG_MARKERS[1])
        return data[last_frame_end + 2:] if last_frame_end != -1 else b''

    def _parse_json(self, data: bytes) -> List[Dict[str, Any]]:
        """Парсинг JSON данных"""
        json_str = self._decode_data(data)
        try:
            parsed = json.loads(json_str)
            return [parsed] if not isinstance(parsed, list) else parsed
        except json.JSONDecodeError:
            return self._parse_fragmented(json_str)

    def _decode_data(self, data: bytes) -> str:
        """Декодирование с учетом кодировок"""
        for encoding in self.ENCODINGS:
            try:
                return data.decode(encoding).strip()
            except UnicodeDecodeError:
                continue
        raise ParserError("Failed to decode JSON data")

    def _parse_fragmented(self, json_str: str) -> List[Dict[str, Any]]:
        """Обработка фрагментированного JSON"""
        result = []
        buffer = ""
        depth = 0

        for char in json_str:
            buffer += char
            if char == '{':
                depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0:
                    try:
                        result.append(json.loads(buffer))
                        buffer = ""
                    except json.JSONDecodeError:
                        logger.warning("Invalid JSON fragment")
        return result

    def _build_violation(self, frames: List[str], data: List[Dict]) -> ViolationData:
        """Создание объекта нарушения"""
        if not frames or not data:
            raise ParserError("Insufficient data to build violation")

        violation = ViolationData()
        last_data = data[-1]

        # Обработка изображений
        violation.v_photo_ts = frames[0]
        violation.v_photo_extra = frames[1:] if len(frames) > 1 else []

        # Заполнение данных
        self._fill_from_sections(
            violation,
            last_data.get('device_info', {}),
            last_data.get('installation_place_info', {}),
            last_data.get('violation_info', {}),
            last_data.get('recogniser_info', {})
        )

        return violation

    def _fill_from_sections(self, violation: ViolationData,
                            device: Dict, place: Dict,
                            violation_info: Dict, recogniser: Dict):
        """Заполнение данных из всех секций"""
        self._fill_device(violation, device)
        self._fill_place(violation, place)
        self._fill_violation(violation, violation_info)
        self._fill_recogniser(violation, recogniser)

    def _fill_device(self, violation: ViolationData, data: Dict):
        """Данные устройства"""
        violation.v_camera = data.get('name_speed_meter')
        violation.v_camera_serial = data.get('factory_number')

    def _fill_place(self, violation: ViolationData, data: Dict):
        """Данные места"""
        violation.v_camera_place = data.get('place', '')
        violation.v_direction = "Попутное" if data.get('direction') == 0 else "Встречное"
        violation.v_direction_name = data.get('place_outcoming', '')
        violation.v_gps_x = self._parse_float(data.get("latitude"))
        violation.v_gps_y = self._parse_float(data.get("longitude"))

    def _fill_violation(self, violation: ViolationData, data: Dict):
        """Данные нарушения"""
        violation.v_time_check = self._parse_datetime(data)
        violation.v_speed = self._parse_int(data.get('speed'))
        violation.v_speed_limit = self._parse_int(data.get('speed_threshold'))
        violation.v_car_type = data.get('type')
        violation.v_patrol_speed = self._parse_int(data.get('self_speed'), 0)

        if reason := data.get('crime_reason'):
            violation.v_pr_viol = [reason]

    def _fill_recogniser(self, violation: ViolationData, data: Dict):
        """Данные распознавания"""
        violation.v_regno = (data.get('plate_chars') or '').replace("|", "")
        violation.v_regno_country_id = data.get('plate_code')

        mark = data.get('mark', '')
        model = data.get('model', '')
        violation.v_ts_model = f"({mark}/{model})" if mark or model else None

    def _parse_float(self, value: Any) -> float:
        """Парсинг float значений"""
        try:
            if isinstance(value, str):
                value = re.sub(r"[^\d.-]", "", value)
            return float(value) if value else self.default_coord
        except (ValueError, TypeError):
            return self.default_coord

    def _parse_int(self, value: Any, default: int = None) -> Optional[int]:
        """Парсинг int значений"""
        try:
            return int(value) if value is not None else default
        except (ValueError, TypeError):
            return default

    def _parse_datetime(self, data: Dict) -> str:
        """Парсинг временных меток"""
        try:
            timestamp = data.get('UTC')
            if not timestamp:
                return self.default_date.isoformat(timespec='milliseconds')

            dt = datetime.utcfromtimestamp(timestamp)
            ms = self._parse_int(data.get('ms'), 0)
            tz_offset = self._parse_int(data.get('timezone'), 0) * 360

            dt += timedelta(
                milliseconds=ms,
                hours=tz_offset // 3600
            )
            return dt.isoformat(timespec='milliseconds')
        except Exception:
            return self.default_date.isoformat(timespec='milliseconds')