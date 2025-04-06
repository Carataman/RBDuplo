import base64
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class ParserError(Exception):
    """Кастомное исключение для ошибок парсинга"""
    pass


class JpegParser:
    """Парсер JPEG файлов с встроенными метаданными о нарушениях"""

    JPEG_MARKERS = (b'\xff\xd8', b'\xff\xd9')  # Маркеры начала/конца JPEG
    ENCODINGS = ['utf-8', 'windows-1251', 'latin-1']  # Поддерживаемые кодировки

    def __init__(self, config: Optional[Dict] = None):
        """Инициализация парсера с конфигурацией"""
        self.config = config or {}
        self._setup_defaults()
        self._load_field_config()

    def _setup_defaults(self):
        """Установка значений по умолчанию"""
        self.default_coord = 0.0
        self.default_speed = 0
        self.default_date_str = datetime(1970, 1, 1).isoformat()

    def _load_field_config(self):
        """Загрузка конфигурации полей"""
        try:
            if 'field_config' in self.config:
                self.field_config = self.config['field_config'].get('violation_fields', {})
            else:
                config_path = Path('convertor/conf/field_config.json')
                with open(config_path) as f:
                    self.field_config = json.load(f).get('violation_fields', {})
        except Exception as e:
            logger.error(f"Failed to load field config: {e}")
            self.field_config = {
                'required': ['v_regno', 'v_time_check', 'v_photo_ts'],
                'optional': {},
                'field_mapping': {}
            }

    def parse(self, jpeg_data: bytes) -> Optional[Dict]:
        """Основной метод парсинга"""
        try:
            self._validate_input(jpeg_data)
            jpeg_frames = self._extract_frames(jpeg_data)
            json_data = self._extract_json(jpeg_data)
            parsed_data = self._parse_json(json_data)

            violation_data = self._build_violation(jpeg_frames, parsed_data)
            return self._ensure_serializable(violation_data)

        except ParserError as e:
            logger.error(f"Parser validation error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected parser error: {e}", exc_info=True)
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
            if start_pos == -1:
                break

            end_pos = data.find(self.JPEG_MARKERS[1], start_pos)
            if end_pos == -1:
                break

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

    def _build_violation(self, frames: List[str], data: List[Dict]) -> Dict:
        """Создание объекта нарушения с защитой от всех ошибок формата"""
        violation = {}
        if not frames or not data:
            return violation

        try:
            last_data = data[-1]

            # Обработка изображений
            violation['v_photo_ts'] = frames[0]
            if len(frames) > 1:
                violation['v_photo_extra'] = frames[1:]

            # Обработка временной метки
            violation_info = last_data.get('violation_info', {})
            violation['v_time_check'] = self._parse_timestamp(violation_info)

            # Обработка координат GPS с защитой от некорректных значений
            if 'installation_place_info' in last_data:
                place_data = last_data['installation_place_info']
                violation['v_gps_x'] = self._parse_coordinate(place_data.get('latitude'))
                violation['v_gps_y'] = self._parse_coordinate(place_data.get('longitude'))

            # Обработка данных распознавания
            if 'recogniser_info' in last_data:
                recognizer_data = last_data['recogniser_info']
                violation.update(self._parse_recognizer_data(recognizer_data))

            # Остальные поля
            if 'device_info' in last_data:
                device_data = last_data['device_info']
                violation.update({
                    'v_camera': device_data.get('name_speed_meter'),
                    'v_camera_serial': device_data.get('factory_number')
                })

            return violation
        except Exception as e:
            logger.error(f"Error building violation: {e}")
            return {}

    def _parse_timestamp(self, data: Dict) -> str:
        """Парсинг временной метки с защитой от ошибок"""
        try:
            utc_timestamp = data.get('UTC')
            if not utc_timestamp:
                return datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

            dt = datetime.utcfromtimestamp(float(utc_timestamp))
            milliseconds = int(data.get('ms', 0))
            timezone_offset = int(data.get('timezone', 0)) * 360
            dt += timedelta(
                milliseconds=milliseconds,
                hours=timezone_offset // 3600
            )
            return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        except Exception as e:
            logger.error(f"Timestamp parsing error: {e}")
            return datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

    def _ensure_serializable(self, data: Dict) -> Dict:
        """Гарантирует, что все данные сериализуемы в JSON"""

        def convert(obj):
            if isinstance(obj, (datetime, date, time)):
                return obj.isoformat()
            elif isinstance(obj, (float, int, str, bool)) or obj is None:
                return obj
            elif isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [convert(item) for item in obj]
            else:
                return str(obj)

        try:
            # Двойная конвертация для проверки
            json_str = json.dumps(data, default=convert)
            return json.loads(json_str)
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            return {}

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

    def _parse_recognizer_data(self, data: Dict) -> Dict:
        """Обработка данных распознавания"""
        result = {
            'v_regno': data.get('plate_chars', '').replace("|", ""),
            'v_regno_country_id': data.get('plate_code')
        }

        # Обработка модели ТС
        mark = data.get('mark', '')
        model = data.get('model', '')
        if mark or model:
            result['v_ts_model'] = f"({mark}/{model})"

        return result

    def _ensure_serializable(self, data: Dict) -> Dict:
        """Гарантированная сериализация с обработкой datetime"""
        try:
            # Преобразуем все datetime в строки
            serialized = json.dumps(data, default=self._json_serializer)
            return json.loads(serialized)
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            return {}

    @staticmethod
    def _json_serializer(obj: Any) -> Any:
        """Универсальный сериализатор"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        try:
            return str(obj)
        except Exception:
            return None
    @staticmethod
    def _parse_int(value: Any, default: int = None) -> Optional[int]:
        """Парсинг int значений"""
        try:
            return int(value) if value is not None else default
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _parse_float(value: Any) -> float:
        """Парсинг float значений"""
        try:
            if isinstance(value, str):
                value = re.sub(r"[^\d.-]", "", value)
            return float(value) if value else 0.0
        except (ValueError, TypeError):
            return 0.0