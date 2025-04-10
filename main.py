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
        try:
            violations = self.db.get_new_violations()
            if not violations:
                logger.info("Новых нарушений не найдено")
                return

            logger.info(f"Найдено {len(violations)} новых нарушений для обработки")

            for violation in violations:
                self._process_single_violation(violation)

            # Обновляем дату последней обработки
            self._update_last_processed_date()

        except Exception as e:
            logger.error(f"Ошибка при обработке нарушений: {e}", exc_info=True)
        finally:
            self.db.close()
            self.ftp.disconnect()

    def _process_single_violation(self, violation: dict) -> bool:
        """Обработка одного нарушения"""
        violation_id = violation['id']
        file_path = violation['file_path']

        try:
            logger.info(f"Обработка нарушения {violation_id} (файл: {file_path})")

            # 1. Загрузка файла с FTP
            file_data = self.ftp.download(file_path)
            if not file_data:
                logger.error(f"Не удалось загрузить файл {file_path}")
                return False

            # 2. Парсинг JPEG и извлечение данных
            parser = JpegParser()
            parsed_data = parser.parse(file_data)
            if not parsed_data:
                logger.error(f"Не удалось распарсить данные из файла {file_path}")
                return False

            # 3. Подготовка данных для API
            violation_data = parser._parse_json()
            if not violation_data:
                logger.error("Не удалось подготовить данные для отправки")
                return False

            # 4. Отправка данных на сервер
            try:
                success = self.api.send_violation(data=violation_data)

                if success:
                    logger.info(f"Нарушение {violation_id} успешно обработано")
                else:
                    logger.error(f"Ошибка при отправке нарушения {violation_id}")

                return success

            except Exception as e:
                logger.error(f"Ошибка при отправке нарушения {violation_id}: {e}")
                return False

        except Exception as e:
            logger.error(f"Ошибка при обработке нарушения {violation_id}: {e}", exc_info=True)
            return False

    def _update_last_processed_date(self):
        """Обновление даты последней обработки в конфиге"""
        new_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.config['processing']['start_date'] = new_date
        logger.info(f"Обновлена дата последней обработки: {new_date}")


if __name__ == "__main__":
    processor = ViolationProcessor()
    processor.process_violations()