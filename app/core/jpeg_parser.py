import base64
import json
import logging
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from app.models import models
logger = logging.getLogger(__name__)

class JpegParser:
    def __init__(self, file_path: str = None):  # Делаем параметр необязательным
        self.file_path = file_path
        self.parsed_objects = []
        self.file_data = None
        self.segments = []
        self.fr_segments = []

    def _extract_first_jpeg_segment(self, file_data: bytes) -> bytes:
        """Извлекает первую JPEG последовательность между маркерами FF D8 и FF D9"""
        start_pos = file_data.find(b'\xff\xd8')
        if start_pos == -1:
            raise ValueError("Не найден начальный маркер JPEG (FF D8)")

        end_pos = file_data.find(b'\xff\xd9', start_pos)
        if end_pos == -1:
            raise ValueError("Не найден конечный маркер JPEG (FF D9)")

        return file_data[start_pos:end_pos + 2]

    def _extract_jpeg_segments(self, file_data: bytes) -> List[bytes]:
        """Извлекает все JPEG сегменты из файла."""
        segments = []
        pos = 0
        start_marker = b'\xff\xd8'
        end_marker = b'\xff\xd9'

        while True:
            start_pos = file_data.find(start_marker, pos)
            if start_pos == -1:
                break

            end_pos = file_data.find(end_marker, start_pos)
            if end_pos == -1:
                break

            segments.append(file_data[start_pos:end_pos + 2])
            pos = end_pos + 2

        logger.info(f"Найдено {len(segments)} JPEG сегментов")
        self.segments = segments
        return segments

    def segments_to_base64(self, segments: List[bytes]) -> List[str]:
        """Конвертирует список бинарных сегментов в base64"""
        return [base64.b64encode(seg).decode('utf-8') for seg in segments]





    def parse(self, file_data: bytes) -> Optional[Dict[str, Any]]:
        self.file_data = file_data
        self._extract_jpeg_segments(file_data)
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

            if self.segments:
                # Преобразуем все сегменты в base64
                photo_extra_base64 = self.segments_to_base64(self.segments)

                # Формируем правильную структуру для photo_main
                photo_main1 =  photo_extra_base64[0]
                photo_main2 = {"vtypePhoto": 1, "vphotoExtra": photo_extra_base64[2]}

            else:
                photo_main1 = []
                photo_main2 = []
            common_extra = [photo_main2]
            #общий шаблон
            result = {
                "v_azimut": None,
                "v_camera": data.get("device_info", {}).get("name_speed_meter"),
                "v_camera_serial": data.get("device_info", {}).get("factory_number"),
                "v_camera_place": data.get("installation_place_info", {}).get("place"),
                "v_direction": direct,
                "v_direction_name": None,
                "v_gps_x": gps_x,
                "v_gps_y": v_gps_y,
                "v_photo_ts": photo_main1,
                "v_pr_viol": [data.get("violation_info", {}).get("crime_reason")],
                "v_regno": data.get("recogniser_info", {}).get("plate_chars", "").replace("|", ""),
                "v_regno_country_id": models.Abbreviation.get_country_code(data.get("recogniser_info", {}).get("plate_code")),
                "v_speed": data.get("violation_info", {}).get("speed"),
                "v_self_speed": data.get("violation_info", {}).get("self_speed"),
                "v_speed_limit": data.get("installation_place_info", {}).get("speed_limit"),
                "v_time_check": self._parse_timestamp(data),
                "v_ts_type": models.Abbreviation.get_vehicle_type(data.get("violation_info", {}).get("type")),
                "v_ts_model": f"{data.get('recogniser_info', {}).get('mark', '')}/{data.get('recogniser_info', {}).get('model', '')}",
                "photo_extra": common_extra
            }

            return result

        except Exception as e:
            logger.error(f"Ошибка при разборе JSON: {e}")
            return None


