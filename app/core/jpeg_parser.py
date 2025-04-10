import json
import logging
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class JpegParser:
    def __init__(self, file_path: str = None):  # Делаем параметр необязательным
        self.file_path = file_path
        self.parsed_objects = []

    #def parse(self, file_data: bytes) -> Optional[Dict[str, Any]]:
    def parse(self, file_data: bytes) -> Optional[Dict[str, Any]]:
        """Извлекает JSON-данные из JPEG-файла."""
        try:
            # Находим конец JPEG (маркер 0xFFD9)
            jpeg_end = file_data.rfind(b"\xff\xd9")
            if jpeg_end == -1:
                raise ValueError("Не найден конец JPEG-файла")

            json_data = file_data[jpeg_end + 2:]  # Данные после маркера

            # Декодируем (пробуем UTF-8, затем Windows-1251)
            try:
                json_str = json_data.decode("utf-8").strip()
            except UnicodeDecodeError:
                json_str = json_data.decode("windows-1251").strip()

            # Исправляем разорванные JSON-объекты (если разделены "}{")
            json_objects = json_str.strip().split("}{")
            json_objects = [
                "{" + obj if not obj.startswith("{") else obj for obj in json_objects
            ]
            json_objects = [
                obj + "}" if not obj.endswith("}") else obj for obj in json_objects
            ]

            # Парсим JSON
            self.parsed_objects = []
            for obj in json_objects:
                try:
                    self.parsed_objects.append(json.loads(obj))
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка при разборе JSON: {e}")
                    continue

            return self.parsed_objects if self.parsed_objects else None

        except Exception as e:
            logger.error(f"Ошибка при чтении файла: {e}")
            return None

    def _parse_coordinate(self, coord: Any) -> float:
        """Парсинг координат с обработкой специальных форматов"""
        if coord is None:
            return 0.0

        try:
            if isinstance(coord, str):
                # Обработка формата типа 'N0.000000'
                coord = re.sub(r'[^0-9.-]', '', coord)
                if not coord:  # Если после очистки строка пустая
                    return 0.0
            return float(coord)
        except (ValueError, TypeError):
            return 0.0

    def _parse_timestamp(self, data: Dict) -> str:
        """Парсинг временной метки с защитой от ошибок"""
        try:
            utc_timestamp = data.get("violation_info", {}).get("UTC")
            if not utc_timestamp:
                return datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

            dt = datetime.utcfromtimestamp(float(utc_timestamp))
            milliseconds = int(data.get("violation_info", {}).get("ms", 0))
            timezone_offset = int(data.get("violation_info", {}).get("timezone", 0)) * 360
            dt += timedelta(
                milliseconds=milliseconds,
                hours=timezone_offset // 3600
            )
            return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        except Exception as e:
            logger.error(f"Timestamp parsing error: {e}")
            return datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

    def _parse_json(self) -> Optional[Dict[str, Any]]:
        """Извлекает структурированные данные из JSON."""
        if not self.parsed_objects:
            logger.error("Нет данных для разбора")
            return None

        try:
            # Берём первый JSON-объект (можно доработать для множества)
            data = self.parsed_objects[1]
            gps_x = (data.get("installation_place_info", {}).get("latitude")).replace("N", "")
            v_gps_y = (data.get("installation_place_info", {}).get("longitude")).replace("E", "")

            #определение направления
            if  data.get("violation_info", {}).get("direction") == 0:
                direct = "Встречное"
                direct_name = data.get("installation_place_info", {}).get("place_incoming")
            else:
                direct = "Попутное"
                direct_name = data.get("installation_place_info", {}).get("place_outcoming")

            #общий шаблон
            result = {
                "v_azimut": None,
                "v_camera": data.get("device_info", {}).get("name_speed_meter"),
                "v_camera_serial": data.get("device_info", {}).get("factory_number"),
                "v_camera_place": data.get("installation_place_info", {}).get("place"),
                "v_direction": direct ,
                "v_direction_name": None,
                "v_gps_x": gps_x,
                "v_gps_y": v_gps_y,
                "v_photo_ts": None,
                "v_pr_viol": [data.get("violation_info", {}).get("crime_reason")],
                "v_regno": data.get("recogniser_info", {}).get("plate_chars", "").replace("|", ""),
                "v_regno_country_id": data.get("recogniser_info", {}).get("plate_code"),
                "v_speed": data.get("violation_info", {}).get("speed"),
                "v_self_speed": data.get("violation_info", {}).get("self_speed"),
                "v_speed_limit": data.get("installation_place_info", {}).get("speed_limit"),
                "v_time_check": self._parse_timestamp(data),
                "v_ts_type": data.get("violation_info", {}).get("type"),
                "v_ts_model": f"{data.get('recogniser_info', {}).get('mark', '')}/{data.get('recogniser_info', {}).get('model', '')}",
                "photo_extra": [None],
            }

            return result

        except Exception as e:
            logger.error(f"Ошибка при разборе JSON: {e}")
            return None


