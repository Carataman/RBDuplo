import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from app.core.database import DatabaseConnect
from app.core.ftp_client import FTPClient
from app.core.jpeg_parser import JpegParser
from app.services.api_service import SendToServer

logger = logging.getLogger(__name__)


class DataProcessingService:
    def __init__(self, config: dict):
        self.config = config
        self._validate_config()
        self._init_components()

    def _validate_config(self):
        """Проверка обязательных секций конфига"""
        required_sections = {
            'database': "Не найдена конфигурация базы данных",
            'ftp': "Не найдена конфигурация FTP",
            'api': "Не найдена конфигурация API",
            'processing': "Не найдена конфигурация обработки"
        }

        for section, error_msg in required_sections.items():
            if section not in self.config:
                raise ValueError(error_msg)

    def _init_components(self):
        """Инициализация компонентов системы"""
        self.start_date = self.config['processing']['start_date']


        self.db = DatabaseConnect(
            config=self.config['database'],
            start_date=self.start_date
        )
        self.ftp = FTPClient(self.config['ftp'])
        self.api = SendToServer(self.config['api'])
        self.parser = JpegParser({
            **self.config.get('parsing', {}),

        })


    def run_processing_flow(self):
        """Основной workflow обработки нарушений"""
        try:
            violations = self.db.get_new_violations()
            logger.info(f"Найдено новых нарушений: {len(violations)}")

            for violation in violations:
                try:
                    self._process_single_violation(violation)
                except Exception as e:
                    logger.error(f"Ошибка обработки нарушения {violation.get('id')}: {e}")
        finally:
            self._cleanup()

    def _process_single_violation(self, violation: Dict):
        """Обработка одного нарушения"""
        photo_data = self.ftp.download(violation['file_path'])
        parsed_data = self.parser.parse(photo_data) or {}

        enriched_data = self._prepare_enriched_data(violation, parsed_data)

        if self._validate_violation(enriched_data):
            self.api.send_violation(enriched_data)
            self.db.mark_as_processed(violation['id'])



    # def _validate_violation(self, data: dict) -> bool:
    #     """Валидация обязательных полей"""
    #     required_fields = self.field_config.get('required', [])
    #     if not all(field in data for field in required_fields):
    #         missing = [f for f in required_fields if f not in data]
    #         logger.error(f"Отсутствуют обязательные поля: {missing}")
    #         return False
    #     return True

    def _enrich_data(self, parsed_data: dict) -> dict:
        """Обогащение данных с проверкой обязательных полей"""

        enriched = {}



        return self._ensure_serializable(enriched)

    # def _ensure_serializable(self, data: dict) -> dict:
    #     """Рекурсивно проверяет и преобразует данные"""
    #     try:
    #         # Проверяем сериализуемость через временное преобразование
    #         test_json = json.dumps(data, default=json_serializer)
    #         return json.loads(test_json)
    #     except (TypeError, ValueError) as e:
    #         logger.error(f"Serialization error: {e}")
    #         return self._deep_convert(data)

    # def _deep_convert(self, obj: Any) -> Any:
    #     """Рекурсивно преобразует все элементы"""
    #     if isinstance(obj, dict):
    #         return {k: self._deep_convert(v) for k, v in obj.items()}
    #     elif isinstance(obj, (list, tuple)):
    #         return [self._deep_convert(item) for item in obj]
    #     elif isinstance(obj, (datetime.datetime, datetime.date)):
    #         return obj.isoformat()
    #     elif hasattr(obj, '__dict__'):
    #         return self._deep_convert(obj.__dict__)
    #     return obj

    # def run_processing_flow(self):
    #     """Основной workflow с обработкой ошибок сериализации"""
    #     try:
    #         violations = self.db.get_new_violations()
    #         logger.info(f"Found {len(violations)} new violations")
    #
    #         for violation in violations:
    #             try:
    #                 # Получаем и обрабатываем данные
    #                 photo_data = self.ftp.download(violation['file_path'])
    #                 parsed_data = self.parser.parse(photo_data) or {}
    #                 enriched_data = self._enrich_data(violation, parsed_data)
    #
    #                 # Дополнительная проверка перед отправкой
    #                 if self._validate_data(enriched_data):
    #                     serialized_data = json.dumps(enriched_data, default=json_serializer)
    #                     #self.api.send_violation(serialized_data)
    #                     self.api.send_violation(enriched_data)
    #                     #self.db.mark_as_processed(violation['id'])
    #
    #             except Exception as e:
    #                 logger.error(f"Failed to process violation {violation.get('id')}: {e}")
    #     finally:
    #         self._cleanup()

    # def _validate_data(self, data: dict) -> bool:
    #     """Проверка данных перед отправкой"""
    #     try:
    #         json.dumps(data, default=json_serializer)
    #         return True
    #     except Exception as e:
    #         logger.error(f"Invalid data format: {e}\nData: {data}")
    #         return False

    def _cleanup(self):
        """Очистка ресурсов"""
        try:
            self.db.close()
            self.ftp.disconnect()
        except Exception as e:
            logger.error(f"Ошибка при очистке ресурсов: {e}")