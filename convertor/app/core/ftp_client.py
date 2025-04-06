import logging
from ftplib import FTP
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, Any
import os

logger = logging.getLogger(__name__)


class FTPClient:
    """Класс для работы с FTP-сервером"""

    def __init__(self, config: Dict[str, str]):
        """
        Args:
            config: Словарь с параметрами подключения:
                   {'ftp_host', 'ftp_user', 'ftp_pass', 'timeout?'}
        """
        self.config = config
        self.connection = None

    def connect(self) -> bool:
        """Устанавливает соединение с FTP-сервером"""
        try:
            self.connection = FTP(
                host=self.config['ftp_host'],
                timeout=self.config.get('timeout', 30))
            self.connection.login(
                user=self.config['ftp_user'],
                passwd=self.config['ftp_pass']
            )
            logger.info("FTP подключение установлено")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения FTP: {e}")
            return False

    def download_file(self, remote_path: str) -> Optional[bytes]:
        """Загружает файл с FTP в память"""
        if not self.connection:
            if not self.connect():
                return None

        try:
            # Нормализация пути
            normalized_path = remote_path.replace("/mnt/targets/ftp/all_fixations", '/')
            dirname = os.path.dirname(normalized_path)
            filename = os.path.basename(normalized_path)

            # Переход в директорию
            self.connection.cwd(dirname)

            # Проверка существования файла
            if filename not in self.connection.nlst():
                logger.warning(f"Файл {filename} не найден в {dirname}")
                return None

            # Загрузка файла
            with BytesIO() as file_data:
                self.connection.retrbinary(f'RETR {filename}', file_data.write)
                logger.info(f"Загружен файл {filename} ({len(file_data.getvalue())} байт)")
                return file_data.getvalue()

        except Exception as e:
            logger.error(f"Ошибка загрузки файла: {e}", exc_info=True)
            return None

    def disconnect(self):
        """Закрывает соединение с FTP"""
        if self.connection:
            try:
                self.connection.quit()
                logger.info("FTP соединение закрыто")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии FTP: {e}")
            finally:
                self.connection = None