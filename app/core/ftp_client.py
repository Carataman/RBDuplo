import logging
from ftplib import FTP
from io import BytesIO

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

    def download(self, base_remote_path: str) -> Dict[str, Optional[bytes]]:
        """
        Загружает все версии файла с разными расширениями (_fr.jpg и .jpg)

        Args:
            base_remote_path: Путь к файлу без суффикса (например, '/path/to/file')

        Returns:
            Словарь с данными файлов: {'original': bytes, 'fr': bytes}
        """
        if not self.connection and not self.connect():
            return {'original': None, 'fr': None}

        try:
            # Нормализация базового пути
            normalized_path = base_remote_path.replace("/mnt/targets/ftp/all_fixations", '/')
            dirname = os.path.dirname(normalized_path)
            base_filename = os.path.basename(normalized_path)

            # Переход в директорию
            self.connection.cwd(dirname)

            # Получаем список всех файлов в директории
            files_in_dir = self.connection.nlst()

            # Формируем полные имена файлов
            original_file = f"{base_filename}.jpg"
            fr_file = f"{base_filename}_fr.jpg"

            result = {'original': None, 'fr': None}

            # Загрузка оригинального файла
            if original_file in files_in_dir:
                with BytesIO() as file_data:
                    self.connection.retrbinary(f'RETR {original_file}', file_data.write)
                    result['original'] = file_data.getvalue()
                    logger.info(f"Загружен оригинальный файл {original_file}")

            # Загрузка _fr версии
            if fr_file in files_in_dir:
                with BytesIO() as file_data:
                    self.connection.retrbinary(f'RETR {fr_file}', file_data.write)
                    result['fr'] = file_data.getvalue()
                    logger.info(f"Загружен FR-файл {fr_file}")

            return result

        except Exception as e:
            logger.error(f"Ошибка загрузки файлов: {e}", exc_info=True)
            return {'original': None, 'fr': None}
    # def download(self, remote_path: str) -> Optional[bytes]:
    #     """Загружает файл с FTP в память"""
    #     if not self.connection:
    #         if not self.connect():
    #             return None
    #
    #     try:
    #         # Нормализация пути
    #         normalized_path = remote_path.replace("/mnt/targets/ftp/all_fixations", '/')
    #         dirname = os.path.dirname(normalized_path)
    #         filename = os.path.basename(normalized_path)
    #
    #         # Переход в директорию
    #         self.connection.cwd(dirname)
    #
    #         # Проверка существования файла
    #         if filename not in self.connection.nlst():
    #             logger.warning(f"Файл {filename} не найден в {dirname}")
    #             return None
    #
    #         # Загрузка файла
    #         with BytesIO() as file_data:
    #             self.connection.retrbinary(f'RETR {filename}', file_data.write)
    #             logger.info(f"Загружен файл {filename} ({len(file_data.getvalue())} байт)")
    #             return file_data.getvalue()
    #
    #     except Exception as e:
    #         logger.error(f"Ошибка загрузки файла: {e}", exc_info=True)
    #         return None

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