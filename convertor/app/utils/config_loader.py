import json
from pathlib import Path
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


def load_config() -> Dict[str, Any]:
    """Загружает все конфиги из папки conf"""
    try:
        # Определяем правильный путь к папке conf
        current_file = Path(__file__).absolute()
        project_root = current_file.parent.parent.parent  # Поднимаемся на 3 уровня вверх
        conf_dir = project_root / "conf"

        logger.debug(f"Ищем конфиги в: {conf_dir}")

        if not conf_dir.exists():
            raise FileNotFoundError(f"Папка conf не найдена по пути: {conf_dir}")

        config = {}

        # Загрузка connection.json
        conn_path = conf_dir / "connection.json"
        if conn_path.exists():
            with open(conn_path, 'r', encoding='utf-8') as f:
                config.update(json.load(f))
            logger.info(f"Успешно загружен {conn_path}")
        else:
            raise FileNotFoundError(f"Файл connection.json не найден в {conf_dir}")

        # Загрузка configuration.json
        conf_path = conf_dir / "configuration.json"
        if conf_path.exists():
            with open(conf_path, 'r', encoding='utf-8') as f:
                config.update(json.load(f))
            logger.info(f"Успешно загружен {conf_path}")
        else:
            logger.warning(f"Файл configuration.json не найден, используются значения по умолчанию")
            config.setdefault('start_date', '1970-01-01')

        # Валидация обязательных параметров
        if 'database' not in config:
            raise ValueError("Отсутствует секция 'database' в конфигурации")

        return config

    except json.JSONDecodeError as e:
        logger.error(f"Ошибка формата JSON в конфиге: {e}")
        raise
    except Exception as e:
        logger.error(f"Критическая ошибка загрузки конфигов: {e}")
        raise RuntimeError(f"Не удалось загрузить конфигурацию: {e}")