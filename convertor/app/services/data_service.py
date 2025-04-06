from typing import Dict
from ..core.jpeg_parser import JpegParser


class DataService:
    def __init__(self, db_connector, ftp_client, parser_config):
        self.db = db_connector
        self.ftp = ftp_client
        self.parser = JpegParser(parser_config)

    def process_violation(self, violation: Dict) -> Dict:
        """Полный цикл обработки нарушения"""
        # 1. Загрузка фото
        photo_data = self.ftp.download(violation['photo_path'])

        # 2. Парсинг данных
        parsed_data = self.parser.parse(photo_data)

        # 3. Формирование результата
        return {
            **violation,
            **parsed_data,
            "status": "processed"
        }