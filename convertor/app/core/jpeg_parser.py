import base64
import json
import logging
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class ParserError(Exception):
    """Кастомные ошибки парсинга"""
    pass


class JpegParser:
    """Парсер JPEG с метаданными о нарушениях"""

    JPEG_MARKERS = (b'\xff\xd8', b'\xff\xd9')  # Маркеры начала/конца JPEG
    ENCODINGS = ['utf-8', 'windows-1251', 'latin-1']  # Поддерживаемые кодировки

    # Маппинг полей: server_field -> json_path
    FIELD_MAPPING = {
        "v_camera": "device_info.name_speed_meter",
        "v_camera_serial": "device_info.factory_number",
        "v_camera_place": "installation_place_info.place",
        "v_regno": "recogniser_info.plate_chars",
        "v_regno_country_id": "recogniser_info.plate_code",
        "v_gps_x": "installation_place_info.latitude",
        "v_gps_y": "installation_place_info.longitude",
        "v_speed": "violation_info.speed",
        "v_speed_limit": "installation_place_info.speed_limit",
        "v_patrol_speed": "violation_info.liplate_speed",
        "v_pr_viol": "violation_info.type",
        "v_direction": "violation_info.direction",
        "v_azimut": "installation_info.rotate_angle",
        "v_ts_model": "recogniser_info.model",
        "v_ts_type": "recogniser_info.mark"
    }

    def __init__(self):
        """Инициализация парсера (без параметров)"""
        pass

    def parse(self, jpeg_data: bytes) -> Optional[Dict]:
        """Основной метод парсинга"""
        try:
            self._validate_input(jpeg_data)
            frames = self._extract_frames(jpeg_data)
            json_data = self._extract_json(jpeg_data)
            parsed_json = self._parse_json(json_data)
            return self._build_violation(frames, parsed_json)
        except ParserError as e:
            logger.error(f"Validation error: {e}")
            return None
        except Exception as e:
            logger.error(f"Critical parsing error: {e}", exc_info=True)
            return None

    def _validate_input(self, data: bytes):
        """Проверка валидности входных данных"""
        if not data or len(data) < 4:
            raise ParserError("Invalid or empty input data")
        if not data.startswith(self.JPEG_MARKERS[0]):
            raise ParserError("Not a valid JPEG file")

    def _extract_frames(self, data: bytes) -> List[str]:
        """Извлекает JPEG фреймы в base64"""
        frames = []
        pos = 0
        while pos < len(data):
            start = data.find(self.JPEG_MARKERS[0], pos)
            if start == -1: break

            end = data.find(self.JPEG_MARKERS[1], start)
            if end == -1: break

            frame = data[start:end + 2]
            frames.append(base64.b64encode(frame).decode('utf-8'))
            pos = end + 2
        return frames

    def _extract_json(self, data: bytes) -> bytes:
        """Извлекает JSON данные из конца файла"""
        last_marker = data.rfind(self.JPEG_MARKERS[1])
        return data[last_marker + 2:] if last_marker != -1 else b''

    def _parse_json(self, data: bytes) -> List[Dict]:
        """Парсинг JSON с обработкой фрагментированных данных"""
        try:
            json_str = self._decode_data(data)
            return json.loads(json_str) if json_str else []
        except json.JSONDecodeError:
            return self._parse_fragmented_json(json_str)

    def _decode_data(self, data: bytes) -> str:
        """Декодирование с учетом разных кодировок"""
        for encoding in self.ENCODINGS:
            try:
                return data.decode(encoding).strip()
            except UnicodeDecodeError:
                continue
        raise ParserError("Failed to decode JSON data")

    def _parse_fragmented_json(self, json_str: str) -> List[Dict]:
        """Обработка битых JSON данных"""
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
                        logger.warning("Invalid JSON fragment: %s", buffer[:50])
        return result

    def _build_violation(self, frames: List[str], data: List[Dict]) -> Dict:
        """Формирует объект нарушения через маппинг полей"""
        try:
            merged_data = self._merge_json_data(data)
            result = {}

            # Основной маппинг полей
            for server_field, json_path in self.FIELD_MAPPING.items():
                raw_value = self._get_nested_value(merged_data, json_path)
                result[server_field] = self._parse_field(server_field, raw_value)

            # Специальная обработка
            result.update({
                "v_time_check": self._parse_timestamp(merged_data.get("violation_info", {})),
                "v_direction_name": self._parse_direction(result.get("v_direction", 1)),
                "v_photo_ts": frames[0] if frames else "",
                "photo_extra": frames[1:] if len(frames) > 1 else []
            })

            return self._ensure_serializable(result)
        except Exception as e:
            logger.error(f"Build violation error: {e}", exc_info=True)
            return {}

    def _parse_field(self, field: str, raw_value: Any) -> Any:
        """Преобразование сырых данных в целевой формат"""
        try:
            if field in ["v_gps_x", "v_gps_y"] and isinstance(raw_value, str):
                return self._parse_coordinate(raw_value)

            if field.startswith("v_speed"):
                return float(raw_value) if raw_value else 0.0

            if field == "v_pr_viol":
                return [self._parse_violation_type(raw_value)] if raw_value else []

            return raw_value
        except Exception as e:
            logger.warning(f"Parse error for {field}: {e}")
            return None

    def _parse_timestamp(self, data: Dict) -> str:
        """Конвертация Unix timestamp в ISO 8601"""
        try:
            utc_ts = data["UTC"]
            ms = data.get("ms", 0)
            dt = datetime.fromtimestamp(utc_ts, tz=timezone.utc)
            return (dt + timedelta(milliseconds=ms)).isoformat(timespec="milliseconds")
        except Exception as e:
            logger.error(f"Timestamp error: {e}")
            return datetime.now(timezone.utc).isoformat()

    def _parse_direction(self, direction_code: int) -> str:
        """Преобразование кода направления в текст"""
        directions = {
            0: "Попутное",
            1: "Встречное",
            2: "Оба направления"
        }
        return directions.get(direction_code, "Неизвестно")

    def _parse_violation_type(self, type_code: int) -> str:
        """Преобразование кода нарушения в текст"""
        types = {
            1: "Превышение скорости",
            2: "Проезд на красный",
            3: "Непредоставление преимущества"
        }
        return types.get(type_code, "Неизвестное нарушение")

    def _parse_coordinate(self, coord: str) -> float:
        """Обработка координат вида N51.870209"""
        try:
            return float(coord[1:]) if coord else 0.0
        except (ValueError, TypeError) as e:
            logger.error(f"Coordinate error: {e}")
            return 0.0

    def _merge_json_data(self, data: List[Dict]) -> Dict:
        """Объединение JSON объектов в один словарь"""
        merged = {}
        for obj in data:
            for key, value in obj.items():
                if isinstance(value, dict):
                    merged.setdefault(key, {}).update(value)
                else:
                    merged[key] = value
        return merged

    def _get_nested_value(self, data: Dict, path: str) -> Any:
        """Получение значения из вложенной структуры"""
        keys = path.split('.')
        current = data
        for key in keys:
            current = current.get(key, {}) if isinstance(current, dict) else None
            if current is None:
                return None
        return current or None

    def _ensure_serializable(self, data: Dict) -> Dict:
        """Конвертация всех объектов в JSON-совместимые типы"""
        try:
            return json.loads(json.dumps(data, default=self._json_serializer))
        except TypeError as e:
            logger.error(f"Serialization error: {e}")
            return {}

    @staticmethod
    def _json_serializer(obj: Any) -> str:
        """Сериализация datetime объектов"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)