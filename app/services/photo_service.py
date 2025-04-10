from typing import Optional, Dict
from app.core.ftp_client import FTPClient
from  app.utils.config_loader import load_config


class PhotoService:
    """Сервис для работы с фотографиями"""

    def __init__(self, config: Optional[Dict] = None):
        """
        Args:
            config: Если None, конфиг будет загружен автоматически
        """
        self.config = config or load_config().get('ftp', {})
        self.ftp_client = FTPClient(self.config)

    def download_photo(self, remote_path: str) -> Optional[bytes]:
        """Загружает фото с FTP"""
        return self.ftp_client.download_file(remote_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ftp_client.disconnect()