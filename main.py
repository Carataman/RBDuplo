# main.py (исправленная версия)
import logging
from datetime import datetime
import tempfile
import os
from typing import Optional

from app.core.database import DatabaseConnect
from app.core.ftp_client import FTPClient
from app.core.jpeg_parser import JpegParser
from app.services.api_service import SendToServer
from app.utils.config_loader import load_config
from app.utils.logger import setup_logging

logger = logging.getLogger(__name__)


class ViolationProcessor:
    def __init__(self):
        setup_logging()
        self.config = load_config()
        self.db = DatabaseConnect(self.config['database'], self.config['processing']['start_date'])
        self.ftp = FTPClient(self.config['ftp'])
        self.api = SendToServer(self.config['api'])

    def process_violations(self):
        """Основной цикл обработки нарушений"""
        violations = self.db.get_new_violations()

        for violation in violations:
            try:
                logger.info(f"Обработка нарушения ID: {violation['id']}")

                # Загрузка обоих файлов
                files_data = self.ftp.download_pair(violation['files']['original']['path'])

                if not files_data['original']:
                    logger.error("Не удалось загрузить основное фото")
                    continue

                # Парсинг данных
                parser = JpegParser()
                parsed_data = parser.parse(files_data['fr'])

                if not parsed_data:
                    logger.error("Не удалось распарсить данные")
                    continue

                # Подготовка данных для API
                violation_data = {
                    **parser._parse_json(),

                }

                # Отправка на сервер
                success = self.api.send_violation(violation_data)

                if success:
                    logger.info(f"Нарушение {violation['id']} успешно обработано")

                else:
                    logger.error(f"Ошибка отправки нарушения {violation['id']}")

            except Exception as e:
                logger.error(f"Ошибка обработки нарушения {violation['id']}: {e}")




if __name__ == "__main__":
    processor = ViolationProcessor()
    processor.process_violations()
