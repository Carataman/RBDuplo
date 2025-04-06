import base64
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any

from app.core.models import ViolationData  # Модель данных из core/models.py
from app.utils.exceptions import ParserError

logger = logging.getLogger(__name__)


class JpegParser:
    """Класс для парсинга JPEG файлов с метаданными нарушений"""

    def __init__(self, config: Optional[Dict] = None):
        """
        Args:
            config: Конфигурация парсера (может содержать настройки парсинга)
        """
        self.config = config or {}

    def parse(self, jpeg_data: bytes) -> Optional[ViolationData]:
        """Основной метод парсинга JPEG данных"""
        try:
            # Извлекаем все JPEG изображения
            jpeg_frames = self._extract_jpeg_frames(jpeg_data)
            if not jpeg_frames:
                raise ParserError("No JPEG frames found in data")

            # Извлекаем JSON данные
            json_data = self._extract_json_data(jpeg_data)
            if not json_data:
                raise ParserError("No JSON metadata found")

            # Парсим JSON
            parsed_objects = self._parse_json_data(json_data)
            if not parsed_objects:
                raise ParserError("No valid JSON objects found")

            # Создаем и заполняем объект нарушения
            violation = self._create_violation(jpeg_frames, parsed_objects[-1])
            return violation

        except Exception as e:
            logger.error(f"Failed to parse JPEG: {str(e)}", exc_info=True)
            return None

    def _extract_jpeg_frames(self, data: bytes) -> List[str]:
        """Извлекает JPEG кадры и конвертирует в base64"""
        frames = []
        pos = 0
        start_marker = b'\xff\xd8'
        end_marker = b'\xff\xd9'

        while pos < len(data):
            start_pos = data.find(start_marker, pos)
            if start_pos == -1:
                break

            end_pos = data.find(end_marker, start_pos)
            if end_pos == -1:
                break

            frame = data[start_pos:end_pos + 2]
            frames.append(base64.b64encode(frame).decode('utf-8'))
            pos = end_pos + 2

        return frames

    def _extract_json_data(self, data: bytes) -> Optional[bytes]:
        """Извлекает JSON метаданные после JPEG"""
        last_jpeg_end = data.rfind(b'\xff\xd9')
        return data[last_jpeg_end + 2:] if last_jpeg_end != -1 else None

    def _parse_json_data(self, json_data: bytes) -> List[Dict[str, Any]]:
        """Парсит JSON данные с обработкой разных кодировок"""
        json_str = self._decode_json_data(json_data)
        return self._parse_json_string(json_str)

    def _decode_json_data(self, data: bytes) -> str:
        """Декодирует байты в строку с учетом разных кодировок"""
        for encoding in ['utf-8', 'windows-1251', 'latin-1']:
            try:
                return data.decode(encoding).strip()
            except UnicodeDecodeError:
                continue
        raise ParserError("Failed to decode JSON data")

    def _parse_json_string(self, json_str: str) -> List[Dict[str, Any]]:
        """Парсит JSON строку, обрабатывая возможные ошибки"""
        try:
            parsed = json.loads(json_str)
            return [parsed] if not isinstance(parsed, list) else parsed
        except json.JSONDecodeError:
            return self._parse_fragmented_json(json_str)

    def _parse_fragmented_json(self, json_str: str) -> List[Dict[str, Any]]:
        """Обрабатывает фрагментированный JSON"""
        json_objects = []
        buffer = ""
        brace_count = 0

        for char in json_str:
            buffer += char
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    try:
                        json_objects.append(json.loads(buffer))
                        buffer = ""
                    except json.JSONDecodeError:
                        logger.warning("Failed to parse JSON fragment")
        return json_objects

    def _create_violation(self, jpeg_frames: List[str], data: Dict[str, Any]) -> ViolationData:
        """Создает объект ViolationData из распарсенных данных"""
        violation = ViolationData()

        # Заполнение фото
        violation.v_photo_ts = jpeg_frames[0]
        violation.v_photo_extra = jpeg_frames[1:] if len(jpeg_frames) > 1 else []

        # Извлекаем данные из JSON структуры
        device_info = data.get('device_info', {})
        place_info = data.get('installation_place_info', {})
        violation_info = data.get('violation_info', {})
        recogniser_info = data.get('recogniser_info', {})

        # Заполняем основные поля
        self._fill_device_info(violation, device_info)
        self._fill_place_info(violation, place_info)
        self._fill_violation_info(violation, violation_info)
        self._fill_recogniser_info(violation, recogniser_info)

        return violation

    def _fill_device_info(self, violation: ViolationData, data: Dict[str, Any]):
        """Заполняет информацию об устройстве"""
        violation.v_camera = data.get('name_speed_meter')
        violation.v_camera_serial = data.get('factory_number')

    def _fill_place_info(self, violation: ViolationData, data: Dict[str, Any]):
        """Заполняет информацию о месте установки"""
        violation.v_camera_place = data.get('place')
        violation.v_direction = "Попутное" if data.get('direction') == 0 else "Встречное"
        violation.v_direction_name = data.get('place_outcoming')
        violation.v_gps_x = self._parse_coordinate(data.get("latitude", "0"))
        violation.v_gps_y = self._parse_coordinate(data.get("longitude", "0"))

    def _fill_violation_info(self, violation: ViolationData, data: Dict[str, Any]):
        """Заполняет информацию о нарушении"""
        violation.v_time_check = self._parse_timestamp(data)
        violation.v_speed = data.get('speed')
        violation.v_speed_limit = data.get('speed_threshold')
        violation.v_car_type = data.get('type')
        violation.v_patrol_speed = data.get('self_speed')
        violation.v_pr_viol = [data.get('crime_reason')]

    def _fill_recogniser_info(self, violation: ViolationData, data: Dict[str, Any]):
        """Заполняет информацию о распознавании"""
        violation.v_regno = data.get('plate_chars', '').replace("|", "")
        violation.v_regno_country_id = data.get('plate_code')
        violation.v_ts_model = f"({data.get('mark')}/{data.get('model')})"

    def _parse_coordinate(self, coord_str: str) -> float:
        """Парсит координату из строки"""
        if not coord_str:
            return 0.0
        try:
            clean_str = re.sub(r"[^\d.-]", "", coord_str)
            return float(clean_str)
        except (ValueError, TypeError):
            return 0.0

    def _parse_timestamp(self, data: Dict[str, Any]) -> str:
        """Парсит и форматирует временную метку"""
        utc_timestamp = data.get('UTC')
        if not utc_timestamp:
            return datetime.now().isoformat(timespec='milliseconds')

        dt = datetime.utcfromtimestamp(utc_timestamp)
        milliseconds = data.get('ms', 0)
        timezone_offset = data.get('timezone', 0) * 360

        dt += timedelta(
            milliseconds=milliseconds,
            hours=timezone_offset // 3600
        )

        return dt.isoformat(timespec='milliseconds')