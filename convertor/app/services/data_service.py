import logging
from datetime import datetime
from typing import Dict, Any
from convertor.app.core.database import DatabaseConnect
from convertor.app.core.ftp_client import FTPClient
from convertor.app.core.jpeg_parser import JpegParser
from convertor.app.services.api_service import SendToServer
logger = logging.getLogger(__name__)


class DataProcessingService:
    def __init__(self, config: dict):  # Принимаем config как параметр
        self.config = config  # Сохраняем конфиг в объекте
        self._initialize_components()

    def _initialize_components(self):
        """Инициализация компонентов с проверкой конфигов"""
        self.db = DatabaseConnect(self.config['database'])
        required_configs = {
            'database': "Не найдена конфигурация базы данных",
            'ftp': "Не найдена конфигурация FTP",
            'api': "Не найдена конфигурация API"
        }

        # Проверяем наличие всех необходимых секций
        for key, error_msg in required_configs.items():
            if key not in self.config:
                raise ValueError(error_msg)

        # Инициализация компонентов
        self.db = DatabaseConnect(self.config['database'])
        self.ftp = FTPClient(self.config['ftp'])
        self.api = SendToServer(self.config['api'])
        self.parser = JpegParser(self.config.get('parsing', {}))



    def run_processing_flow(self):
        """Основной workflow приложения"""
        try:
            # 1. Получаем новые нарушения из БД
            violations = self.db.get_new_violations()
            logger.info(f"Found {len(violations)} new violations")

            for violation in violations:
                try:
                    # 2. Загружаем фото с FTP
                    photo_data = self.ftp.download(violation['file_path'])

                    # 3. Парсим JPEG
                    parsed_data = self.parser.parse(photo_data)
                    if not parsed_data:
                        continue

                    # 4. Обогащаем данные
                    enriched_data = self._enrich_data(violation, parsed_data)

                    # 5. Отправляем на сервер
                    self.api.send_violation(enriched_data)

                    # 6. Помечаем как обработанное
                    self.db.mark_as_processed(violation['id'])

                except Exception as e:
                    logger.error(f"Failed to process violation {violation['id']}: {e}")
                    continue

        finally:
            self._cleanup()

    def _enrich_data(self, db_data: Dict, parsed_data: Dict) -> Dict:
        """Объединение данных из БД и распарсенных данных"""
        return {
            **db_data,
            **parsed_data,
            'processing_time': datetime.now().isoformat()
        }

    def _cleanup(self):
        """Очистка ресурсов"""
        self.db.close()
        self.ftp.disconnect()