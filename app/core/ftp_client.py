import logging
from ftplib import FTP
from io import BytesIO

from typing import Optional, Dict, Any
import os

logger = logging.getLogger(__name__)


class FTPClient:
    """Класс для работы с FTP-сервером"""

    def __init__(self, config: Dict[str, str]):
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

    def download_pair(self, base_remote_path: str) -> Dict[str, Optional[bytes]]:
        """
        Загружает оба файла (основной и _fr версию) с FTP
        Возвращает словарь {'original': bytes, 'fr': bytes}
        """
        if not self.connection and not self.connect():
            return {'original': None, 'fr': None}

        try:
            # Нормализация базового пути (без расширения)
            normalized_path = base_remote_path.replace("/mnt/targets/ftp/all_fixations", '')
            base_name = os.path.splitext(os.path.basename(normalized_path))[0]
            dir_path = os.path.dirname(normalized_path)

            # Переход в директорию
            self.connection.cwd(dir_path)
            files_in_dir = self.connection.nlst()

            result = {'original': None, 'fr': None}

            # Формируем имена файлов
            original_file = f"{base_name}.jpg"
            fr_file = f"{base_name}_fr.jpg"

            # Загрузка оригинального файла
            if original_file in files_in_dir:
                with BytesIO() as file_data:
                    self.connection.retrbinary(f'RETR {original_file}', file_data.write)
                    result['original'] = file_data.getvalue()
                    logger.info(f"Загружен оригинальный файл {original_file} ({len(result['original'])} байт)")

            # Загрузка _fr версии
            if fr_file in files_in_dir:
                with BytesIO() as file_data:
                    self.connection.retrbinary(f'RETR {fr_file}', file_data.write)
                    result['fr'] = file_data.getvalue()
                    logger.info(f"Загружен FR-файл {fr_file} ({len(result['fr'])} байт)")

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
    #         filename_main = os.path.basename(normalized_path)
    #         filename_fr = os.path.basename(normalized_path)
    #
    #         # Переход в директорию
    #         self.connection.cwd(dirname)
    #
    #         # Проверка существования файла
    #         if filename_main and filename_fr not in self.connection.nlst():
    #             logger.warning(f"Файл {filename_main} не найден в {dirname}")
    #             logger.warning(f"Файл {filename_fr} не найден в {dirname}")
    #
    #             return None
    #
    #         # Загрузка файла
    #         with BytesIO() as file_data:
    #             self.connection.retrbinary(f'RETR {filename_main}', file_data.write)
    #             logger.info(f"Загружен файл {filename_main} ({len(file_data.getvalue())} байт)")
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

